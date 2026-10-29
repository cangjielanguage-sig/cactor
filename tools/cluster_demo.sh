#!/usr/bin/env bash
# 多进程集群验证：启动 3 个独立进程组成集群，经分片收发消息并校验，然后依次离开。
#   tools/cluster_demo.sh            # 需要先在 workspace 根目录 cjpm build
set -uo pipefail
cd "$(dirname "$0")/.."
if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi
BIN=./target/release/bin/cactor_examples.cluster_node
BASE=${CLUSTER_DEMO_PORT:-25510}
SEED="127.0.0.1:${BASE}"
N=3
logdir=$(mktemp -d)
pids=()
CACTOR_LOG_LEVEL=error "$BIN" "$BASE" "$SEED" "$N" driver > "$logdir/node0.log" 2>&1 &
pids+=($!)
for i in $(seq 1 $((N - 1))); do
    CACTOR_LOG_LEVEL=error "$BIN" "$((BASE + i))" "$SEED" "$N" > "$logdir/node$i.log" 2>&1 &
    pids+=($!)
done
status=0
for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then
        status=1
    fi
done
cat "$logdir"/node*.log | grep -v '^\s*$'
rm -rf "$logdir"
if [[ $status -eq 0 ]]; then echo "全部进程正常退出"; else echo "有进程失败"; fi
exit $status
