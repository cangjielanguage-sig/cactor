#!/bin/bash
# CActor 性能基线门禁（perf20261101.md M0/P6 + 20261116.md E53）
#
# 运行 scale_test / real_benchmark / scale_latency，与本机基线比较，超过阈值即退出码 1。
#
# 用法：
#   tools/bench_gate.sh                          # 用本机基线 + 基线里记录的阈值/采样次数
#   tools/bench_gate.sh 10                        # 覆盖阈值（百分数）
#   tools/bench_gate.sh 10 1                      # 覆盖阈值与采样次数
#   tools/bench_gate.sh --update                  # 实测并写入本机基线（只在独立提交里做）
#   tools/bench_gate.sh --update 25 3             # 同上，显式指定阈值与采样次数
#
# 基线按机器指纹分文件：bench/results/baseline-<os>-<arch>-<cores>.json
# 不同机器的核心数/架构不同，吞吐量级差数倍，共用一份基线只会得到"换台机器就红"。
# 找不到本机基线时以退出码 2 结束并打印期望的文件名——这是"门禁真的在用基线"的证明。
# 需要临时改用别的基线：CACTOR_BENCH_BASELINE=/path/to/baseline.json。
#
# 阈值默认值按机器噪声标定：桌面机 25%（load 常年 20-80，墙钟波动实测可达 ±15-20%），
# 静止的 CI 机器可以在该机器的基线文件里写 10。±5% 只会把噪声当回归。
# 采样次数内取最优值（吞吐取最大、时延/时长取最小），与 bench 方法论的"交替跑取最好值"一致。
# 内存（heap_bytes_per_actor）最稳定，不受负载影响（GC region 粒度噪声约 ±15%）。
#
# 前置：workspace 根目录已 cjpm build；需要 jq（仓颉环境未 source 时脚本自动 source）。
set -u
cd "$(dirname "$0")/.."

# 自带仓颉环境（与 ci.sh 一致）：否则在未 source envsetup.sh 的 shell 里，每个可执行程序都因
# 找不到 libcangjie-runtime.so 以 127 退出，整张矩阵全红（E26）
# 仓颉环境 + DYLD_*（macOS）/ LD_LIBRARY_PATH（Linux）统一由 tools/env.sh 给出。
# 不要只依赖继承来的环境：经 `guard`（timeout + bash）或 CI 的 /bin/bash 调用时，
# dyld 共享缓存里的平台二进制 bash 会把继承的 DYLD_* 全部剔除，脚本自己不再设一遍就会
# 找不到 libcangjie-runtime.dylib（"no LC_RPATH's found"，Abort trap: 6）。
# env.sh 幂等（重复 source 不会重复追加路径），且包含 CANGJIE_HOME 探测与 OpenSSL 3 兜底。
# shellcheck disable=SC1091
source tools/env.sh

UPDATE=0
if [ "${1:-}" = "--update" ]; then
    UPDATE=1
    shift
fi

BIN="./target/release/bin"
FAIL=0

command -v jq >/dev/null 2>&1 || { echo "需要 jq"; exit 2; }

# machine_fingerprint：os-arch-cores。
# os 统一小写（Darwin → darwin），核数用逻辑核（与调度线程数相关）。
# macOS 用 sysctl，Linux 用 nproc / /proc/cpuinfo，最后退回 getconf。
machine_fingerprint() {
    local os arch cores
    os=$(uname -s | tr '[:upper:]' '[:lower:]')
    arch=$(uname -m)
    if command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu >/dev/null 2>&1; then
        cores=$(sysctl -n hw.ncpu)
    elif command -v nproc >/dev/null 2>&1; then
        cores=$(nproc)
    else
        cores=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)
    fi
    printf '%s-%s-%s' "$os" "$arch" "$cores"
}

MACHINE=$(machine_fingerprint)
EXPECTED_BASELINE="bench/results/baseline-${MACHINE}.json"
BASELINE="${CACTOR_BENCH_BASELINE:-$EXPECTED_BASELINE}"

if [ "$UPDATE" = "0" ]; then
    if [ ! -f "$BASELINE" ]; then
        cat >&2 <<EOF
缺少本机性能基线：$BASELINE
  - 机器指纹：$MACHINE（os-arch-cores）
  - 期望文件：$EXPECTED_BASELINE
  - 生成方式：tools/bench_gate.sh --update   （需要空闲机器，结果写入独立提交）
  - CI 镜像首次接入时同样先跑 --update，或在 CACTOR_BENCH_BASELINE 指向已有基线
EOF
        exit 2
    fi
    # 阈值与采样次数优先取命令行，其次取基线文件，最后退回默认值。
    THRESHOLD="${1:-$(jq -r '.threshold_pct // empty' "$BASELINE")}"
    REPEATS="${2:-$(jq -r '.repeats // empty' "$BASELINE")}"
    THRESHOLD="${THRESHOLD:-25}"
    REPEATS="${REPEATS:-3}"
else
    THRESHOLD="${1:-$([ -f "$BASELINE" ] && jq -r '.threshold_pct // empty' "$BASELINE")}"
    REPEATS="${2:-$([ -f "$BASELINE" ] && jq -r '.repeats // empty' "$BASELINE")}"
    THRESHOLD="${THRESHOLD:-25}"
    REPEATS="${REPEATS:-3}"
fi

base() { jq -r "$1" "$BASELINE"; }

# best <方向> <值列表>：higher 取最大，lower 取最小。
# 只在数值样本中取最优：空样本（运行崩溃 / 输出缺行）不能参与排序——
# 此前 sort 把空串排在最前，lower 方向取到空串误报"未取得实测值"，higher 方向则把崩溃静默吞掉（E26）。
# 崩溃本身由 ran() 按退出码判失败。
best() {
    local dir="$1"; shift
    if [ "$dir" = "higher" ]; then
        printf '%s\n' "$@" | grep -E '^[0-9]+(\.[0-9]+)?$' | sort -g | tail -1
    else
        printf '%s\n' "$@" | grep -E '^[0-9]+(\.[0-9]+)?$' | sort -g | head -1
    fi
}

# ran <名称> <退出码>：基准进程非 0 退出（崩溃、超时 124、OOM）即判门禁失败
ran() {
    if [ "$2" -ne 0 ]; then
        echo "✗ $1 运行失败（退出码 $2）"
        FAIL=1
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
    if [ -z "$expected" ] || [ "$expected" = "null" ]; then
        echo "✗ $name: 基线缺少该指标（用 --update 重新生成）"
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

if [ "$UPDATE" = "1" ]; then
    echo "=== bench_gate --update：机器 $MACHINE，阈值 ±${THRESHOLD}%，每组采样 ${REPEATS} 次取最优 ==="
    echo "    写入 $BASELINE（请单独提交，并在 CHANGELOG 记录机器与负载）"
else
    echo "=== bench_gate：基线 $BASELINE（机器 $MACHINE），阈值 ±${THRESHOLD}%，每组采样 ${REPEATS} 次取最优 ==="
fi

# ---------- 规模（1M 子 Actor）：创建/drain/停止/每 Actor 堆 ----------
echo "--- scale_test 1000000 child ---"
CREATE_LIST=(); DRAIN_LIST=(); STOP_LIST=(); HEAP_LIST=()
for _ in $(seq "$REPEATS"); do
    OUT=$(cjHeapSize=8GB CACTOR_LOG_LEVEL=error timeout 300 \
        "$BIN/cactor_examples.scale_test" 1000000 child 2>&1)
    ran "scale_test" $?
    CREATE_LIST+=("$(echo "$OUT" | grep -oE '创建：[0-9]+ ms' | grep -oE '[0-9]+')")
    DRAIN_LIST+=("$(echo "$OUT" | grep -oE '每个处理一条消息：[0-9]+ ms' | grep -oE '[0-9]+')")
    STOP_LIST+=("$(echo "$OUT" | grep -oE '停止：[0-9]+ ms' | grep -oE '[0-9]+')")
    HEAP_LIST+=("$(echo "$OUT" | grep -oE '存活堆 [0-9]+ 字节' | grep -oE '[0-9]+')")
done
CREATE=$(best lower "${CREATE_LIST[@]}")
DRAIN=$(best lower "${DRAIN_LIST[@]}")
STOP=$(best lower "${STOP_LIST[@]}")
HEAP=$(best lower "${HEAP_LIST[@]}")

# ---------- 常规基准（4 线程）----------
# 不在正则里用多字节字符（BSD grep 的 [^→] 按字节匹配不可靠）：取行内 "N msg/s" 的 N
echo "--- real_benchmark T4 ---"
M1_LIST=(); M2_LIST=(); M6_LIST=(); M7_LIST=(); P50_LIST=(); P99_LIST=()
for _ in $(seq "$REPEATS"); do
    OUT=$(CACTOR_DISPATCHER_THREADS=4 CACTOR_LOG_LEVEL=error timeout 600 "$BIN/cactor_examples.real_benchmark" 2 2>&1)
    ran "real_benchmark" $?
    M1_LIST+=("$(echo "$OUT" | grep '^\[1\]' | tail -1 | grep -oE '[0-9]+ msg/s' | grep -oE '[0-9]+' | head -1)")
    M2_LIST+=("$(echo "$OUT" | grep '^\[2\]' | tail -1 | grep -oE '[0-9]+ msg/s' | grep -oE '[0-9]+' | head -1)")
    M6_LIST+=("$(echo "$OUT" | grep '^\[6\]' | tail -1 | grep -oE '[0-9]+ msg/s' | grep -oE '[0-9]+' | head -1)")
    M7_LIST+=("$(echo "$OUT" | grep '^\[7\]' | tail -1 | grep -oE '[0-9]+ msg/s' | grep -oE '[0-9]+' | head -1)")
    P50_LIST+=("$(echo "$OUT" | grep -oE 'P50=[0-9.]+' | head -1 | cut -d= -f2)")
    P99_LIST+=("$(echo "$OUT" | grep -oE 'P99=[0-9.]+' | head -1 | cut -d= -f2)")
done
M1=$(best higher "${M1_LIST[@]}")
M2=$(best higher "${M2_LIST[@]}")
M6=$(best higher "${M6_LIST[@]}")
M7=$(best higher "${M7_LIST[@]}")
P50=$(best lower "${P50_LIST[@]}")
P99=$(best lower "${P99_LIST[@]}")

# ---------- 常驻延迟 ----------
echo "--- scale_latency 1000000 ---"
LP50_LIST=(); LP99_LIST=()
for _ in $(seq "$REPEATS"); do
    OUT=$(cjHeapSize=8GB CACTOR_LOG_LEVEL=error timeout 300 \
        "$BIN/cactor_examples.scale_latency" 1000000 2>&1)
    ran "scale_latency" $?
    LP50_LIST+=("$(echo "$OUT" | grep -oE 'P50=[0-9.]+' | head -1 | cut -d= -f2)")
    LP99_LIST+=("$(echo "$OUT" | grep -oE 'P99=[0-9.]+' | head -1 | cut -d= -f2)")
done
LP50=$(best lower "${LP50_LIST[@]}")
LP99=$(best lower "${LP99_LIST[@]}")

if [ "$UPDATE" = "1" ]; then
    # 任一指标为空（进程崩溃 / 输出缺行）时不能写基线，否则会把"零"固化下来，
    # 之后所有机器都自动通过。
    for pair in "create:$CREATE" "drain:$DRAIN" "stop:$STOP" "heap:$HEAP" \
                "single:$M1" "fanout:$M2" "multi_pairs:$M6" "typed_pairs:$M7" \
                "p50:$P50" "p99:$P99" "lat_p50:$LP50" "lat_p99:$LP99"; do
        if [ -z "${pair#*:}" ]; then
            echo "✗ 指标 ${pair%%:*} 未取得实测值，拒绝写入基线" >&2
            exit 1
        fi
    done
    if [ "$FAIL" -ne 0 ]; then
        echo "✗ 基准进程有失败，拒绝写入基线" >&2
        exit 1
    fi
    tmp=$(mktemp)
    cat >"$tmp" <<EOF
{
  "machine": "$MACHINE",
  "os": "$(uname -s | tr '[:upper:]' '[:lower:]')",
  "arch": "$(uname -m)",
  "cores": $(echo "$MACHINE" | awk -F- '{print $NF}'),
  "threshold_pct": $THRESHOLD,
  "repeats": $REPEATS,
  "generated_at": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
  "comment": "由 tools/bench_gate.sh --update 生成；性能基线按机器指纹分文件（E53）。机器负载与日期见 generated_at，重测时写 CHANGELOG。",
  "scale_test_child_1000000": {
    "create_ms": $CREATE,
    "drain_ms": $DRAIN,
    "stop_ms": $STOP,
    "heap_bytes_per_actor": $HEAP
  },
  "real_benchmark_t4": {
    "single_actor_mps": $M1,
    "fanout_mps": $M2,
    "multi_pairs_mps": $M6,
    "typed_pairs_mps": $M7,
    "pingpong_p50_us": $P50,
    "pingpong_p99_us": $P99
  },
  "scale_latency_1000000": {
    "p50_us": $LP50,
    "p99_us": $LP99
  }
}
EOF
    jq empty "$tmp" || { echo "✗ 生成的 JSON 非法" >&2; rm -f "$tmp"; exit 1; }
    mv "$tmp" "$BASELINE"
    echo "✓ 已写入 $BASELINE"
    echo "=== bench_gate 完成 ==="
    exit 0
fi

# ---------- 对比 ----------
check "创建 1M 子 Actor (ms)"  "$CREATE" "$(base '.scale_test_child_1000000.create_ms')" lower
check "drain 1M (ms)"          "$DRAIN"  "$(base '.scale_test_child_1000000.drain_ms')"  lower
check "停止 1M 子 Actor (ms)"  "$STOP"   "$(base '.scale_test_child_1000000.stop_ms')"   lower
check "每 Actor 存活堆 (B)"    "$HEAP"   "$(base '.scale_test_child_1000000.heap_bytes_per_actor')" lower
check "[1] 单 Actor 吞吐 (msg/s)"  "$M1" "$(base '.real_benchmark_t4.single_actor_mps')"
check "[2] 扇出吞吐 (msg/s)"       "$M2" "$(base '.real_benchmark_t4.fanout_mps')"
check "[6] 多对 ping-pong (msg/s)" "$M6" "$(base '.real_benchmark_t4.multi_pairs_mps')"
check "[7] 类型化互发 (msg/s)"     "$M7" "$(base '.real_benchmark_t4.typed_pairs_mps')"
check "[3] ping-pong P50 (µs)"     "$P50" "$(base '.real_benchmark_t4.pingpong_p50_us')" lower
check "[3] ping-pong P99 (µs)"     "$P99" "$(base '.real_benchmark_t4.pingpong_p99_us')" lower
check "常驻 1M P50 (µs)" "$LP50" "$(base '.scale_latency_1000000.p50_us')" lower
check "常驻 1M P99 (µs)" "$LP99" "$(base '.scale_latency_1000000.p99_us')" lower

echo "=== bench_gate 完成 ==="
exit $FAIL
