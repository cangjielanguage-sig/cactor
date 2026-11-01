#!/usr/bin/env bash
# 构建并运行 protoactor-go 对照基准（需要 Go 1.25.3+；首次运行会下载依赖）
#   bench/protoactor/run.sh [all|basic|inproc] [对数] [每对消息数]
# 容器里运行时 Go 1.25+ 会按 cgroup CPU 限额设置 GOMAXPROCS，与 CActor 默认的工作线程数规则一致。
set -euo pipefail
cd "$(dirname "$0")"
go build -o protoactor-bench .
./protoactor-bench "$@"
