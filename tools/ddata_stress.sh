#!/usr/bin/env bash
# 分布式数据并发压测连跑（E43：N0 定位与回归）
#
#   tools/ddata_stress.sh [轮数=10] [每轮秒数=8]
#
# 每轮运行 cactor_examples.ddata_stress（3 节点回环 TCP、并发读写、停止后校验收敛），
# 轮流切换四种 GC 配置：默认 / 极频繁 GC（1 ms 间隔、4 MB 阈值）/ 小堆 / 4 KB region。
# 任何一轮进程崩溃（含 SIGSEGV，退出码 139）、未收敛或有解码失败即失败。
# 需要先在 workspace 根目录 cjpm build。
set -uo pipefail
cd "$(dirname "$0")/.."

if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi

ROUNDS="${1:-10}"
SECS="${2:-8}"
BIN=./target/release/bin/cactor_examples.ddata_stress
CFGS=("" "cjGCInterval=1ms cjGCThreshold=4MB" "cjHeapSize=128MB cjGCInterval=1ms" "cjRegionSize=4KB cjGCInterval=10ms")

fail=0
for i in $(seq 1 "$ROUNDS"); do
    cfg="${CFGS[$(( (i - 1) % 4 ))]}"
    out=$(env CACTOR_LOG_LEVEL=error $cfg timeout --foreground $((SECS + 90)) "$BIN" "$SECS" 2>&1)
    rc=$?
    last=$(printf '%s\n' "$out" | grep -E "收敛|通过|失败" | tr '\n' ' ')
    printf '第 %d 轮 [%s] 退出码 %d：%s\n' "$i" "${cfg:-默认 GC}" "$rc" "$last"
    if [ "$rc" -ne 0 ]; then
        fail=$((fail + 1))
        printf '%s\n' "$out" | tail -30
    fi
done
if [ "$fail" -eq 0 ]; then
    echo "ddata 压测 ${ROUNDS} 轮全部通过（无崩溃、全部收敛）"
else
    echo "ddata 压测 ${ROUNDS} 轮中 ${fail} 轮失败"
    exit 1
fi
