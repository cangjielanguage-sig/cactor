#!/usr/bin/env bash
# 持久化日志 kill -9 验证（E8′）：写入进程不停追加（每次 append 返回即已 fsync，随后打印 ACK），
# 在随机时刻 kill -9；部分轮次再给日志尾部追加断电后的零填充 / 写到一半的记录（P6）。
# 每轮之后用新进程恢复并校验：已确认的事件一条不少、序号从 1 连续、内容与序号对应；下一轮写入从恢复出的序号继续。
#   tools/journal_crash_demo.sh      # 需要先在 workspace 根目录 cjpm build
set -uo pipefail
cd "$(dirname "$0")/.."
if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi
BIN=./target/release/bin/cactor_examples.journal_crash
ROUNDS=${JOURNAL_CRASH_ROUNDS:-6}
IDS=4
work=$(mktemp -d)
journal="$work/journal"
acks="$work/acks.log"
status=0
for r in $(seq 1 "$ROUNDS"); do
    CACTOR_LOG_LEVEL=error "$BIN" writer "$journal" "$IDS" 2>>"$acks" >/dev/null &
    writer=$!
    # 随机 0.2–0.8 s 后 kill -9（写入进程正在 append / fsync 的任意位置）
    sleep "0.$(( RANDOM % 7 + 2 ))"
    kill -9 "$writer" 2>/dev/null
    wait "$writer" 2>/dev/null
    case $(( r % 3 )) in
        1) "$BIN" corrupt "$journal" zero > /dev/null ;;
        2) "$BIN" corrupt "$journal" torn > /dev/null ;;
    esac
    echo -n "第 ${r} 轮：" 
    if ! CACTOR_LOG_LEVEL=error "$BIN" verify "$journal" "$acks" "$IDS"; then
        status=1
        break
    fi
done
rm -rf "$work"
if [[ $status -eq 0 ]]; then echo "日志 kill -9 验证：通过（${ROUNDS} 轮）"; else echo "日志 kill -9 验证：失败"; fi
exit $status
