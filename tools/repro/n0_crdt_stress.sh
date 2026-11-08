#!/usr/bin/env bash
# N0 最小复现尝试（E43）：编译 tools/repro/n0_crdt_stress.cj（-O2）并连跑
#
#   tools/repro/n0_crdt_stress.sh [轮数=50] [每次秒数=5] [读线程=8] [模式...（默认 old-out new-out）]
#
# 每轮按四种 GC 配置之一运行（默认 / 极频繁 GC / 256 MB 小堆 / 4 KB region），进程崩溃即记失败并保留日志。
set -uo pipefail
cd "$(dirname "$0")/../.."
if ! command -v cjc >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi
ROUNDS="${1:-50}"; SECS="${2:-5}"; READERS="${3:-8}"
shift $(( $# < 3 ? $# : 3 ))
MODES=("$@"); [ ${#MODES[@]} -eq 0 ] && MODES=(old-out new-out)
WORK=$(mktemp -d)
cjc -O2 -Woff unused tools/repro/n0_crdt_stress.cj -o "$WORK/n0_crdt_stress" || exit 2
CFGS=("" "cjGCInterval=1ms cjGCThreshold=4MB" "cjHeapSize=256MB cjGCInterval=1ms" "cjRegionSize=4KB cjGCInterval=10ms")
fail=0
for i in $(seq 1 "$ROUNDS"); do
    cfg="${CFGS[$(( (i - 1) % 4 ))]}"
    for mode in "${MODES[@]}"; do
        log="$WORK/r$i-$mode.log"
        env $cfg "$WORK/n0_crdt_stress" "$mode" "$SECS" "$READERS" >"$log" 2>&1
        rc=$?
        printf '第 %d 轮 %-8s [%s] 退出码 %d：%s\n' "$i" "$mode" "${cfg:-默认 GC}" "$rc" "$(tail -1 "$log")"
        if [ "$rc" -ne 0 ]; then fail=$((fail + 1)); echo "  日志保留在 $log"; fi
    done
done
echo "N0 压测 ${ROUNDS} 轮 × ${#MODES[@]} 种模式：失败 ${fail} 次"
[ "$fail" -eq 0 ] && rm -rf "$WORK"
[ "$fail" -eq 0 ]
