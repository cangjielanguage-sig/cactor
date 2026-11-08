#!/usr/bin/env bash
# 集群测试套件并行连跑（E43：N0 回归，M2 验收"并行集群套件 50 轮零 SIGSEGV"）
#
#   tools/cluster_parallel_soak.sh [轮数=50] [并行进程数=8]
#
# 直接运行 cactor_tests.unit.cluster 的测试二进制（需先 cjpm test --no-run 或跑过 tools/ci.sh），
# 每轮 --parallel=<进程数>。崩溃与断言失败分开统计：
#   - 崩溃：仓颉运行时打印 "unhandled SIGSEGV"，并行模式下框架把该用例记为
#     "Crashed with exit code 11"（tools 目录外用 raise(11) 验证过这两种输出）；
#   - 断言失败：[ FAILED ] / [ ERROR ] 但无崩溃，通常是本机负载下的时序问题，列出用例名。
# 退出码：有崩溃为 2；只有断言失败为 1；全部通过为 0。
set -uo pipefail
cd "$(dirname "$0")/.."

if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi

ROUNDS="${1:-50}"
PAR="${2:-8}"
BIN=./target/release/unittest_bin/cactor_tests.unit.cluster
if [ ! -x "$BIN" ]; then
    echo "找不到 $BIN，先运行 cjpm test --no-run"
    exit 2
fi
LOG_DIR=$(mktemp -d)
crashes=0
failed_rounds=0
for i in $(seq 1 "$ROUNDS"); do
    log="$LOG_DIR/r$i.log"
    start=$(date +%s)
    env -u CACTOR_LOG_LEVEL "$BIN" --parallel="$PAR" --no-progress --no-color --timeout-each=120s >"$log" 2>&1
    rc=$?
    secs=$(( $(date +%s) - start ))
    crash=$(grep -cE "unhandled SIGSEGV|Crashed with exit code|Segmentation fault" "$log")
    bad=$(grep -E "\[ (FAILED|ERROR) +\] CASE" "$log" | sed 's/.*CASE: //; s/ (.*//' | sort -u | tr '\n' ' ')
    summary=$(grep -E "PASSED:" "$log" | tail -1 | tr -s ' ')
    printf '第 %d 轮 %ds 退出码 %d 崩溃 %d %s %s\n' "$i" "$secs" "$rc" "$crash" "$summary" "$bad"
    if [ "$crash" -gt 0 ]; then
        crashes=$((crashes + 1))
        echo "  崩溃日志保留在 $log"
    elif [ "$rc" -ne 0 ]; then
        failed_rounds=$((failed_rounds + 1))
        echo "  日志保留在 $log"
    else
        rm -f "$log"
    fi
done
echo "并行集群套件 ${ROUNDS} 轮（--parallel=${PAR}）：崩溃 ${crashes} 轮，断言失败 ${failed_rounds} 轮"
if [ "$crashes" -gt 0 ]; then
    exit 2
elif [ "$failed_rounds" -gt 0 ]; then
    exit 1
fi
rm -rf "$LOG_DIR"
