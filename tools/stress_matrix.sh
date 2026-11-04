#!/bin/bash
# CActor 极致压测矩阵（perf20261101.md §7.2）
#
# 覆盖四类压测，全部结果写到 bench/results/<时间戳>/：
#   1. 线程扩展矩阵：CACTOR_DISPATCHER_THREADS = T1/T2/T4/T8/T14 × real_benchmark 的 [1][2][6][7][8][9]，
#      同时记录整进程 user+sys CPU（zsh 内建 time；macOS 上 /proc 不可用）
#   2. 规模扫描：100k / 500k / 1M / 2M 子 Actor 的创建、drain、停止、每 Actor 存活堆
#   3. 长稳 soak：持续收发 + 频繁建停 + 故障重启，检查零丢失与无泄漏
#   4. 竞态与背压：停止竞态套件重复跑 + 三种背压策略
#
# 用法：tools/stress_matrix.sh [quick|full]
#   quick（默认）：线程矩阵 T1/T4/T14，规模扫描 100k/1M，soak 60s
#   full：线程矩阵 T1/T2/T4/T8/T14，规模扫描 100k/500k/1M/2M，soak 300s
#
# 前置：workspace 根目录已 cjpm build。
set -u
cd "$(dirname "$0")/.."

MODE="${1:-quick}"
BIN="./target/release/bin"
STAMP=$(date +%Y%m%d-%H%M%S)
OUT="bench/results/stress-${STAMP}"
mkdir -p "$OUT"
export CACTOR_LOG_LEVEL=error

echo "=== CActor 极致压测矩阵（mode=${MODE}）→ ${OUT} ==="
uptime

# ---------- 1. 线程扩展矩阵 ----------
THREADS_LIST="1 4 14"
SCALES="100000 1000000"
if [ "$MODE" = "full" ]; then
    THREADS_LIST="1 2 4 8 14"
    SCALES="100000 500000 1000000 2000000"
fi

echo
echo "--- 1. 线程扩展矩阵 ---"
for t in $THREADS_LIST; do
    f="$OUT/threads-T${t}.log"
    # 用 zsh 内建 time 取 user+sys（macOS 没有 /proc/self/stat）
    CACTOR_DISPATCHER_THREADS=$t zsh -c "time $BIN/cactor_examples.real_benchmark" > "$f" 2>&1
    echo "T=${t}:"
    grep -E "^\[[126789]\]" "$f" | sed 's/^/    /'
    grep -E "user|sys" "$f" | tail -1 | sed 's/^/    CPU: /'
done

# ---------- 2. 规模扫描 ----------
echo
echo "--- 2. 规模扫描（子 Actor） ---"
for n in $SCALES; do
    f="$OUT/scale-${n}.log"
    cjHeapSize=8GB $BIN/cactor_examples.scale_test "$n" child > "$f" 2>&1
    echo "n=${n}:"
    grep -E "创建：|每个处理一条消息：|停止：|存活堆" "$f" | sed 's/^/    /'
done

# ---------- 3. 长稳 soak ----------
echo
echo "--- 3. 长稳 soak ---"
SOAK_SECS=60
[ "$MODE" = "full" ] && SOAK_SECS=300
f="$OUT/soak-${SOAK_SECS}s.log"
$BIN/cactor_examples.soak_test "$SOAK_SECS" > "$f" 2>&1
soak_rc=$?
tail -20 "$f" | sed 's/^/    /'
echo "    soak 退出码：${soak_rc}"

# ---------- 4. 竞态与背压 ----------
echo
echo "--- 4. 竞态与背压 ---"
# 注 1：本工具链的 cjpm test --filter 未能筛出单个用例（419 个全部被跳过），
#       因此竞态回归靠"整包重复跑"来放大竞态暴露概率。
# 注 2：必须清掉 CACTOR_LOG_LEVEL——上面的压测导出 error 级会传染给测试进程，
#       使日志类用例（结构化日志 / MDC / JSON Lines / 背压日志）的 warn 全部被过滤而误报失败。
for i in 1 2 3; do
    env -u CACTOR_LOG_LEVEL cjpm test > "$OUT/full_test-${i}.log" 2>&1
    echo "    全量测试第 ${i} 次：$(sed 's/\x1b\[[0-9;]*m//g' "$OUT/full_test-${i}.log" | grep -E 'PASSED: [0-9]+, SKIPPED' | tail -1)"
    sed 's/\x1b\[[0-9;]*m//g' "$OUT/full_test-${i}.log" | grep -E "^\s*\[ (ERROR|FAILED) \] CASE" | sed 's/^/        /'
done
f="$OUT/backpressure.log"
$BIN/cactor_examples.best05_backpressure > "$f" 2>&1
echo "    背压示例退出码：$? （输出见 $f）"

echo
echo "=== 结果目录：${OUT} ==="
