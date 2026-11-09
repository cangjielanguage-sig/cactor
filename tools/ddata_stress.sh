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
# 每轮 GC 配置：默认堆 / 极频繁 GC / 小堆 / 4 KB region。
# 注意：机器被其他进程占满 CPU（load 远大于核数）时，GC 线程抢不到时间片，小堆轮次可能以
# "Out of memory" 退出（不是 SIGSEGV、也不是收敛失败）；本研究请先 uptime 看负载。
CFGS=("" "cjGCInterval=1ms cjGCThreshold=4MB" "cjHeapSize=128MB cjGCInterval=1ms" "cjRegionSize=4KB cjGCInterval=10ms")

# Darwin：示例二进制以 @rpath 引用仓颉运行时且未写 LC_RPATH，靠 DYLD_*_LIBRARY_PATH 才能启动。
# 下面的命令经 `env`（SIP 保护的可执行文件）转发，dyld 会把 DYLD_* 从 env 自身的环境里剥掉，
# 所以必须显式再传一次（否则每一轮都在 0 s 以 Abort trap: 6 / "no LC_RPATH's found" 失败）。
DYLD_ENV=()
if [ -n "${DYLD_LIBRARY_PATH:-}" ]; then
    DYLD_ENV=("DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH}")
fi
if [ -n "${DYLD_FALLBACK_LIBRARY_PATH:-}" ]; then
    DYLD_ENV+=("DYLD_FALLBACK_LIBRARY_PATH=${DYLD_FALLBACK_LIBRARY_PATH}")
fi

fail=0
for i in $(seq 1 "$ROUNDS"); do
    cfg="${CFGS[$(( (i - 1) % 4 ))]}"
    out=$(env CACTOR_LOG_LEVEL=error "${DYLD_ENV[@]+"${DYLD_ENV[@]}"}" $cfg timeout --foreground $((SECS + 90)) "$BIN" "$SECS" 2>&1)
    rc=$?
    last=$(printf '%s\n' "$out" | grep -E "收敛|通过|失败" | tr '\n' ' ')
    printf '第 %d 轮 [%s] 退出码 %d：%s\n' "$i" "${cfg:-默认 GC}" "$rc" "$last"
    if [ "$rc" -ne 0 ]; then
        fail=$((fail + 1))
        printf '%s\n' "$out" | tail -30
        if printf '%s' "$out" | grep -q "Out of memory\|OutOfMemoryError"; then
            echo "  提示：本轮是堆耗尽（不是崩溃／断言失败）。机器 CPU 被其他进程占满时 GC 跟不上分配，"
            echo "        可能偶发；先看本轮配置的堆大小与 uptime 的 load average。"
        fi
    fi
done
if [ "$fail" -eq 0 ]; then
    echo "ddata 压测 ${ROUNDS} 轮全部通过（无崩溃、全部收敛）"
else
    echo "ddata 压测 ${ROUNDS} 轮中 ${fail} 轮失败"
    exit 1
fi
