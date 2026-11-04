#!/bin/bash
# CActor 性能基线门禁（perf20261101.md M0/P6）
#
# 运行 scale_test / real_benchmark / scale_latency，与 bench/results/baseline.json 比较，
# 超过阈值即退出码 1。供 CI 或提交前手动运行。
#
# 用法：tools/bench_gate.sh [阈值百分数，默认 25] [每组采样次数，默认 3]
#
# 为什么默认阈值 25% 而不是计划里的 ±5%：本机是桌面环境（load 常年 20-80，Chrome/模拟器/
# 多实例 sidecar 抢核），墙钟指标逐次波动实测可达 ±15-20%。±5% 会把噪声当回归。
# 采样次数内取最优值（吞吐取最大、时延/时长取最小），与 bench 方法论的"交替跑取最好值"一致；
# 静止的 CI 机器可把阈值收紧到 10 并将采样次数设为 1。
# 内存（heap_bytes_per_actor）最稳定，不受负载影响（GC region 粒度噪声约 ±15%）。
#
# 前置：workspace 根目录已 cjpm build；仓颉环境已 source envsetup.sh；需要 jq。
set -u
cd "$(dirname "$0")/.."

THRESHOLD="${1:-25}"
REPEATS="${2:-3}"
BASELINE="bench/results/baseline.json"
BIN="./target/release/bin"
FAIL=0

command -v jq >/dev/null 2>&1 || { echo "需要 jq"; exit 2; }
[ -f "$BASELINE" ] || { echo "缺少基线文件 $BASELINE"; exit 2; }

base() { jq -r "$1" "$BASELINE"; }

# best <方向> <值列表>：higher 取最大，lower 取最小
best() {
    local dir="$1"; shift
    if [ "$dir" = "higher" ]; then
        printf '%s\n' "$@" | sort -g | tail -1
    else
        printf '%s\n' "$@" | sort -g | head -1
    fi
}

# 比较：check <名称> <实测> <基线> [方向] —— 方向 lower=越低越好（时延/时长），缺省 higher
check() {
    local name="$1" actual="$2" expected="$3" dir="${4:-higher}"
    if [ -z "$actual" ] || [ "$actual" = "null" ] || [ "$actual" = "0" ]; then
        echo "✗ $name: 未取得实测值"
        FAIL=1
        return
    fi
    local limit ok
    if [ "$dir" = "lower" ]; then
        limit=$(awk -v e="$expected" -v t="$THRESHOLD" 'BEGIN {printf "%.6g", e * (1 + t/100)}')
        ok=$(awk -v a="$actual" -v l="$limit" 'BEGIN {print (a <= l) ? 1 : 0}')
    else
        limit=$(awk -v e="$expected" -v t="$THRESHOLD" 'BEGIN {printf "%.6g", e * (1 - t/100)}')
        ok=$(awk -v a="$actual" -v l="$limit" 'BEGIN {print (a >= l) ? 1 : 0}')
    fi
    if [ "$ok" = "1" ]; then
        echo "✓ $name: ${actual}（基线 ${expected}，阈值线 ${limit}）"
    else
        echo "✗ $name: ${actual} 越过基线 ${expected} 的 ${THRESHOLD}% 线（${limit}）"
        FAIL=1
    fi
}

echo "=== bench_gate：阈值 ±${THRESHOLD}%，每组采样 ${REPEATS} 次取最优 ==="

# ---------- 规模（1M 子 Actor）：创建/drain/停止/每 Actor 堆 ----------
echo "--- scale_test 1000000 child ---"
CREATE_LIST=(); DRAIN_LIST=(); STOP_LIST=(); HEAP_LIST=()
for _ in $(seq "$REPEATS"); do
    OUT=$(cjHeapSize=8GB CACTOR_LOG_LEVEL=error timeout 300 \
        "$BIN/cactor_examples.scale_test" 1000000 child 2>&1)
    CREATE_LIST+=("$(echo "$OUT" | grep -oE '创建：[0-9]+ ms' | grep -oE '[0-9]+')")
    DRAIN_LIST+=("$(echo "$OUT" | grep -oE '每个处理一条消息：[0-9]+ ms' | grep -oE '[0-9]+')")
    STOP_LIST+=("$(echo "$OUT" | grep -oE '停止：[0-9]+ ms' | grep -oE '[0-9]+')")
    HEAP_LIST+=("$(echo "$OUT" | grep -oE '存活堆 [0-9]+ 字节' | grep -oE '[0-9]+')")
done
check "创建 1M 子 Actor (ms)"  "$(best lower "${CREATE_LIST[@]}")" "$(base '.scale_test_child_1000000.create_ms')" lower
check "drain 1M (ms)"          "$(best lower "${DRAIN_LIST[@]}")"   "$(base '.scale_test_child_1000000.drain_ms')"  lower
check "停止 1M 子 Actor (ms)"  "$(best lower "${STOP_LIST[@]}")"    "$(base '.scale_test_child_1000000.stop_ms')"   lower
check "每 Actor 存活堆 (B)"    "$(best lower "${HEAP_LIST[@]}")"    "$(base '.scale_test_child_1000000.heap_bytes_per_actor')" lower

# ---------- 常规基准（4 线程）----------
# 不在正则里用多字节字符（BSD grep 的 [^→] 按字节匹配不可靠）：取行内 "N msg/s" 的 N
echo "--- real_benchmark T4 ---"
M1_LIST=(); M2_LIST=(); M6_LIST=(); M7_LIST=(); P50_LIST=(); P99_LIST=()
for _ in $(seq "$REPEATS"); do
    OUT=$(CACTOR_DISPATCHER_THREADS=4 CACTOR_LOG_LEVEL=error "$BIN/cactor_examples.real_benchmark" 2 2>&1)
    M1_LIST+=("$(echo "$OUT" | grep '^\[1\]' | tail -1 | grep -oE '[0-9]+ msg/s' | grep -oE '[0-9]+' | head -1)")
    M2_LIST+=("$(echo "$OUT" | grep '^\[2\]' | tail -1 | grep -oE '[0-9]+ msg/s' | grep -oE '[0-9]+' | head -1)")
    M6_LIST+=("$(echo "$OUT" | grep '^\[6\]' | tail -1 | grep -oE '[0-9]+ msg/s' | grep -oE '[0-9]+' | head -1)")
    M7_LIST+=("$(echo "$OUT" | grep '^\[7\]' | tail -1 | grep -oE '[0-9]+ msg/s' | grep -oE '[0-9]+' | head -1)")
    P50_LIST+=("$(echo "$OUT" | grep -oE 'P50=[0-9.]+' | head -1 | cut -d= -f2)")
    P99_LIST+=("$(echo "$OUT" | grep -oE 'P99=[0-9.]+' | head -1 | cut -d= -f2)")
done
check "[1] 单 Actor 吞吐 (msg/s)"  "$(best higher "${M1_LIST[@]}")" "$(base '.real_benchmark_t4.single_actor_mps')"
check "[2] 扇出吞吐 (msg/s)"       "$(best higher "${M2_LIST[@]}")" "$(base '.real_benchmark_t4.fanout_mps')"
check "[6] 多对 ping-pong (msg/s)" "$(best higher "${M6_LIST[@]}")" "$(base '.real_benchmark_t4.multi_pairs_mps')"
check "[7] 类型化互发 (msg/s)"     "$(best higher "${M7_LIST[@]}")" "$(base '.real_benchmark_t4.typed_pairs_mps')"
check "[3] ping-pong P50 (µs)"     "$(best lower "${P50_LIST[@]}")" "$(base '.real_benchmark_t4.pingpong_p50_us')" lower
check "[3] ping-pong P99 (µs)"     "$(best lower "${P99_LIST[@]}")" "$(base '.real_benchmark_t4.pingpong_p99_us')" lower

# ---------- 常驻延迟 ----------
echo "--- scale_latency 1000000 ---"
LP50_LIST=(); LP99_LIST=()
for _ in $(seq "$REPEATS"); do
    OUT=$(cjHeapSize=8GB CACTOR_LOG_LEVEL=error timeout 300 \
        "$BIN/cactor_examples.scale_latency" 1000000 2>&1)
    LP50_LIST+=("$(echo "$OUT" | grep -oE 'P50=[0-9.]+' | head -1 | cut -d= -f2)")
    LP99_LIST+=("$(echo "$OUT" | grep -oE 'P99=[0-9.]+' | head -1 | cut -d= -f2)")
done
check "常驻 1M P50 (µs)" "$(best lower "${LP50_LIST[@]}")" "$(base '.scale_latency_1000000.p50_us')" lower
check "常驻 1M P99 (µs)" "$(best lower "${LP99_LIST[@]}")" "$(base '.scale_latency_1000000.p99_us')" lower

echo "=== bench_gate 完成 ==="
exit $FAIL
