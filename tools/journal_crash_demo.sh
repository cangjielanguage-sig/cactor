#!/usr/bin/env bash
# 持久化日志 kill -9 验证（E8′）：写入进程不停追加（每次 append 返回即已 fsync，随后打印 ACK），
# 在随机时刻 kill -9；部分轮次再给日志尾部追加断电后的零填充 / 写到一半的记录（P6）。
# 每轮之后用新进程恢复并校验：已确认的事件一条不少、序号从 1 连续、内容与序号对应；下一轮写入从恢复出的序号继续。
#   tools/journal_crash_demo.sh      # 需要先在 workspace 根目录 cjpm build
set -uo pipefail
cd "$(dirname "$0")/.."
# 仓颉环境 + DYLD_*（macOS）/ LD_LIBRARY_PATH（Linux）统一由 tools/env.sh 给出。
# 不要只依赖继承来的环境：经 `guard`（timeout + bash）或 CI 的 /bin/bash 调用时，
# dyld 共享缓存里的平台二进制 bash 会把继承的 DYLD_* 全部剔除，脚本自己不再设一遍就会
# 找不到 libcangjie-runtime.dylib（"no LC_RPATH's found"，Abort trap: 6）。
# env.sh 幂等（重复 source 不会重复追加路径），且包含 CANGJIE_HOME 探测与 OpenSSL 3 兜底。
# shellcheck disable=SC1091
source tools/env.sh
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
    # 损坏步骤本身出错（异常退出）也判红，不能让这一轮在没有注入故障的情况下悄悄通过
    mode=""
    case $(( r % 3 )) in
        1) mode=zero ;;
        2) mode=torn ;;
    esac
    if [[ -n $mode ]] && ! "$BIN" corrupt "$journal" "$mode" > /dev/null; then
        echo "第 ${r} 轮：注入 ${mode} 损坏失败"
        status=1
        break
    fi
    echo -n "第 ${r} 轮：" 
    if ! CACTOR_LOG_LEVEL=error "$BIN" verify "$journal" "$acks" "$IDS"; then
        status=1
        break
    fi
done
rm -rf "$work"
if [[ $status -eq 0 ]]; then echo "日志 kill -9 验证：通过（${ROUNDS} 轮）"; else echo "日志 kill -9 验证：失败"; fi
exit $status
