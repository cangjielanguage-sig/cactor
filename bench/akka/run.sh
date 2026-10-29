#!/usr/bin/env bash
# 运行 Akka 对比基准。需要 JDK 17+（使用源码直接运行，不需要 sbt/maven）。
# 用法：bench/akka/run.sh [轮数，默认 3]
set -euo pipefail
cd "$(dirname "$0")"
./fetch-deps.sh > /dev/null
exec java -cp "jars/*" AkkaBench.java "${1:-3}"
