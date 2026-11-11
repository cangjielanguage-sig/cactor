#!/usr/bin/env bash
# 三个独立进程共用持久目录，确认登记、恢复重发及确认删除。
set -euo pipefail
cd "$(dirname "$0")/.."
source tools/env.sh
reminder_demo_dir=$(mktemp -d)
trap 'rm -rf "$reminder_demo_dir"' EXIT
for reminder_demo_mode in write recover verify; do
    CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.durable_reminder_demo "$reminder_demo_mode" "$reminder_demo_dir"
done
