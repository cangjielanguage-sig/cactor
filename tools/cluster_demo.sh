#!/usr/bin/env bash
# 多进程集群验证：启动 3 个独立进程组成集群，经分片收发消息并校验，然后依次离开。
#   tools/cluster_demo.sh            # 需要先在 workspace 根目录 cjpm build
#   tools/cluster_demo.sh --tls      # 全部集群流量走 TLS 1.3 双向认证（E65，测试证书 tools/tls-test-certs，端口 25530 起）
set -uo pipefail
cd "$(dirname "$0")/.."
# 仓颉环境 + DYLD_*（macOS）/ LD_LIBRARY_PATH（Linux）统一由 tools/env.sh 给出。
# 不要只依赖继承来的环境：经 `guard`（timeout + bash）或 CI 的 /bin/bash 调用时，
# dyld 共享缓存里的平台二进制 bash 会把继承的 DYLD_* 全部剔除，脚本自己不再设一遍就会
# 找不到 libcangjie-runtime.dylib（"no LC_RPATH's found"，Abort trap: 6）。
# env.sh 幂等（重复 source 不会重复追加路径），且包含 CANGJIE_HOME 探测与 OpenSSL 3 兜底。
# shellcheck disable=SC1091
source tools/env.sh
BIN=./target/release/bin/cactor_examples.cluster_node
BASE=${CLUSTER_DEMO_PORT:-25510}
if [[ "${1:-}" == "--tls" ]]; then
    export CACTOR_TLS_DIR="$PWD/tools/tls-test-certs"
    BASE=${CLUSTER_DEMO_PORT:-25530}
fi
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
