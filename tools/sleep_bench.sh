#!/usr/bin/env bash
# F1 量化基准（20261117-actor.md §4.7 F1）
#
# 测两件事：
#   1. 睡着会话的唤醒 p99（严格断言 ≤10ms）；
#   2. 睡着会话的内存占用（仅打印：全局堆差在共享机器上被 GC 保留区/线程栈淹没，
#      需要分配器级测量或堆分析才能验证 ≤200B，故不作硬断言）。
#
# 用法：tools/sleep_bench.sh [会话数，默认 2000]
# 需在**空闲机器**上运行，数字才有意义（本机负载高时 p99 会明显偏高）。
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
source tools/env.sh

N="${1:-2000}"

echo "==> 构建测试二进制"
# 强制重链该测试包：cjpm 的增量在“新增/改动测试文件”时可能不刷新 unittest 二进制
rm -f target/release/unittest_bin/cactor_tests.unit.platform.lifecycle \
      target/release/unittest_bin/cactor_tests.unit.platform.lifecycle.cjo
cjpm test -m modules/tests --no-run --no-progress >/dev/null

echo "==> F1 睡眠基准（N=${N}，严格模式：唤醒 p99 ≤ 10ms）"
CACTOR_LOG_LEVEL=error CACTOR_SLEEP_BENCH_STRICT=1 CACTOR_SLEEP_BENCH_N="$N" \
    ./target/release/unittest_bin/cactor_tests.unit.platform.lifecycle \
    --filter='*.*sleepWakeLatencyAndFootprint*'
