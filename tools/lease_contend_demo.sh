#!/usr/bin/env bash
# 跨进程文件租约 + fencing 功能验证（DA4；功能测试，不是压测，约 5 s）：
#   1. 两个 OS 进程在同一共享目录上争抢一个 FileLease：恰好一个持有、另一个被拒；
#   2. kill -9 持有者（不释放租约）：另一个进程在 ttl 过期后才获得，token 更大；
#   3. 用被杀进程的旧 token 写共享状态（模拟停顿后才到达的写入）：被 FileFencingTokenStore 拒绝，状态保持新持有者的值。
#   tools/lease_contend_demo.sh      # 需要先在 workspace 根目录 cjpm build
set -uo pipefail
cd "$(dirname "$0")/.."
if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi
BIN=./target/release/bin/cactor_examples.lease_contend
TTL_MS=2000
work=$(mktemp -d)
pids=()
cleanup() {
    for p in "${pids[@]}"; do kill -9 "$p" 2>/dev/null; wait "$p" 2>/dev/null; done
    rm -rf "$work"
}
trap cleanup EXIT
fail() { echo "失败：$*"; for f in "$work"/*.log; do echo "--- $f"; cat "$f"; done; echo "跨进程租约验证：失败"; exit 1; }

# 轮询等待条件（最多 $1 秒）；脚本只用于多进程编排，测试代码本身不睡固定时长
wait_for() {
    local limit=$1; shift
    local deadline=$(( $(date +%s%N) / 1000000 + limit * 1000 ))
    while (( $(date +%s%N) / 1000000 < deadline )); do
        if "$@"; then return 0; fi
        sleep 0.05
    done
    return 1
}
has() { grep -q "$2" "$work/$1.log" 2>/dev/null; }
token_of() { awk '/^ACQUIRED/ {print $3; exit}' "$work/$1.log"; }
writes_of() { grep -c "^WROTE $1 " "$work/$1.log" 2>/dev/null || true; }

CACTOR_LOG_LEVEL=error "$BIN" hold "$work" p1 "$TTL_MS" 2>"$work/p1.log" & pids+=($!); p1=$!
CACTOR_LOG_LEVEL=error "$BIN" hold "$work" p2 "$TTL_MS" 2>"$work/p2.log" & pids+=($!); p2=$!

one_holds() { { has p1 ACQUIRED && has p2 DENIED; } || { has p2 ACQUIRED && has p1 DENIED; }; }
wait_for 10 one_holds || fail "10 s 内没有出现“一个持有、一个被拒”"
if has p1 ACQUIRED && has p2 ACQUIRED; then fail "两个进程同时持有租约"; fi
if has p1 ACQUIRED; then holder=p1; hpid=$p1; other=p2; else holder=p2; hpid=$p2; other=p1; fi
t1=$(token_of "$holder")
holder_wrote() { [[ $(writes_of "$holder") -ge 2 ]]; }
wait_for 5 holder_wrote || fail "持有者 $holder 没有写入"
echo "互斥：$holder 持有（token $t1），$other 被拒"

kill -9 "$hpid"
wait "$hpid" 2>/dev/null
killed_at=$(( $(date +%s%N) / 1000000 ))
# 被杀进程没有释放租约：另一进程必须等文件里记录的到期时间（最后一次续约 + ttl）过去才能获得
has "$other" ACQUIRED && fail "$other 在持有者被杀之前就拿到了租约"
other_acquired() { has "$other" ACQUIRED; }
wait_for 10 other_acquired || fail "持有者被杀 10 s 后 $other 仍未获得租约"
waited=$(( $(date +%s%N) / 1000000 - killed_at ))
t2=$(token_of "$other")
[[ $t2 -gt $t1 ]] || fail "新 token $t2 不大于旧 token $t1"
# 至少要等过 ttl - 一个心跳间隔（被杀前最后一次续约之后的剩余有效期）
[[ $waited -ge $(( TTL_MS - TTL_MS / 10 - 300 )) ]] || fail "$other 只等了 ${waited} ms 就接手，未等到租约过期"
other_wrote() { [[ $(writes_of "$other") -ge 1 ]]; }
wait_for 5 other_wrote || fail "$other 获得租约后没有写入"
echo "接管：kill -9 $holder 后 ${waited} ms，$other 获得租约（token $t2 > $t1）"

stale=$(CACTOR_LOG_LEVEL=error "$BIN" stale "$work" "$t1") || fail "旧 token 写入未被拒绝：$stale"
show=$(CACTOR_LOG_LEVEL=error "$BIN" show "$work")
[[ $show == "FENCE $t2 STATE $other:$t2 "* ]] || fail "fencing / 状态不符：$show"
echo "fencing：被杀进程的旧 token $t1 写入被拒（$stale），状态仍是 $other 写的值（$show）"
echo "跨进程租约验证：通过"
