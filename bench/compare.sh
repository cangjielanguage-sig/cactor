#!/usr/bin/env bash
# CActor 与 Akka 同机对比：依次运行两边的基准，结果保存到 bench/results/<时间>-<主机>.txt
# 用法：bench/compare.sh [CActor 运行次数，默认 3] [Akka 轮数，默认 3]
# 前置：已 source 仓颉的 envsetup.sh；已安装 JDK 17+
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUNS="${1:-3}"
ROUNDS="${2:-3}"
mkdir -p "$ROOT/bench/results"
OUT="$ROOT/bench/results/$(date +%Y%m%d-%H%M%S)-$(hostname -s).txt"

cd "$ROOT"
cjpm build > /dev/null
{
  echo "# 环境"
  echo "commit: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (有未提交改动)')"
  lscpu | grep -E "Model name|^CPU\(s\)" | sed 's/  */ /g'
  cjc --version 2>&1 | head -1
  java -version 2>&1 | head -1
  echo
  echo "# CActor（${RUNS} 次）"
  for i in $(seq 1 "$RUNS"); do
    ./target/release/bin/cactor_examples.real_benchmark | grep -E "^\[|^ +\["
  done
  echo
  echo "# Akka 2.6.21（${ROUNDS} 轮，取最后一轮）"
  bench/akka/run.sh "$ROUNDS" 2>&1 | grep -E "^---|^\[[0-9]|^ +\[[0-9]"
} | tee "$OUT"
echo
echo "结果已保存: $OUT"
