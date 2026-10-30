#!/usr/bin/env bash
# 多进程宕机验证：3 个独立进程组成集群（KeepMajority 脑裂处理 + remember-entities 分片），
# 实体全部启动后 kill -9 最后一个进程，driver 校验它被移出集群、其上的实体在存活进程上自动重启。
#   tools/cluster_crash_demo.sh      # 需要先在 workspace 根目录 cjpm build
set -uo pipefail
cd "$(dirname "$0")/.."
if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi
BIN=./target/release/bin/cactor_examples.cluster_crash_node
BASE=${CLUSTER_CRASH_PORT:-25520}
SEED="127.0.0.1:${BASE}"
N=3
logdir=$(mktemp -d)
CACTOR_LOG_LEVEL=error "$BIN" "$BASE" "$SEED" "$N" driver > "$logdir/node0.log" 2>&1 &
driver=$!
CACTOR_LOG_LEVEL=error "$BIN" "$((BASE + 1))" "$SEED" "$N" worker > "$logdir/node1.log" 2>&1 &
survivor=$!
CACTOR_LOG_LEVEL=error "$BIN" "$((BASE + 2))" "$SEED" "$N" worker > "$logdir/node2.log" 2>&1 &
victim=$!
# 等 driver 准备好后 kill -9 最后一个进程
for _ in $(seq 1 600); do
    if grep -q READY_TO_KILL "$logdir/node0.log" 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if grep -q READY_TO_KILL "$logdir/node0.log"; then
    kill -9 "$victim" 2>/dev/null
    echo "已 kill -9 进程 127.0.0.1:$((BASE + 2))"
else
    echo "driver 没有就绪"
    kill -9 "$victim" 2>/dev/null
fi
wait "$victim" 2>/dev/null
status=0
wait "$driver" || status=1
wait "$survivor" || status=1
grep -hv -e '^\s*$' -e READY_TO_KILL "$logdir/node0.log" "$logdir/node1.log"
rm -rf "$logdir"
if [[ $status -eq 0 ]]; then echo "存活进程全部正常退出"; else echo "有进程失败"; fi
exit $status
