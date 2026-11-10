#!/usr/bin/env bash
# 进程级故障注入验证（E62 的跨进程部分）：3 个真实进程组集群，经管理端点黑洞掉一条链路，
# 断言故障检测把对端判为不可达，再恢复并断言重新收敛。不靠固定 sleep——所有等待都是轮询端点。
#
#   tools/cluster_chaos.sh              # 需要先在 workspace 根目录 cjpm build
#   CLUSTER_CHAOS_PORT=25610 tools/cluster_chaos.sh
#
# 与 cluster_demo.sh 的区别：那个验证"能正常收发"，这个验证"断了会怎样、恢复后能不能自己好"。
set -uo pipefail
cd "$(dirname "$0")/.."
if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi
source tools/env.sh >/dev/null 2>&1 || true
BIN=./target/release/bin/cactor_examples.cluster_node
BASE=${CLUSTER_CHAOS_PORT:-25610}
MGMT=$((BASE + 100))
SEED="127.0.0.1:${BASE}"
N=3
WAIT=60
logdir=$(mktemp -d)
pids=()

cleanup() {
    for pid in "${pids[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT

fail() {
    echo "失败：$*"
    echo "--- 节点日志 ---"
    cat "$logdir"/node*.log 2>/dev/null | tail -60
    exit 1
}

# 取 JSON 里第一个匹配键的数字/字符串，够用即可（不引入 jq 依赖）
json_num() { sed -n "s/.*\"$1\":\([0-9]*\).*/\1/p" <<<"$2" | head -1; }

status_of() { curl -s --max-time 5 "http://127.0.0.1:$1/cluster/status"; }

wait_until() { # wait_until <描述> <超时秒> <函数>
    local what=$1 deadline=$((SECONDS + $2)) fn=$3
    while (( SECONDS < deadline )); do
        if "$fn"; then
            echo "  ✓ $what"
            return 0
        fi
        sleep 0.2
    done
    fail "${what}（${2}s 超时）"
}

all_converged() {
    local i body
    for i in $(seq 0 $((N - 1))); do
        body=$(status_of "$((MGMT + i))")
        [[ $(json_num upCount "$body") == "$N" ]] || return 1
        grep -q '"converged":true' <<<"$body" || return 1
    done
}

# 单向黑洞：我们发不出去、但还收得到对端心跳，所以是**对端**把我们判为不可达——这正是不对称故障的样子
node2_reports_node0_unreachable() {
    status_of "$((MGMT + 2))" | grep -q "\"unreachable\":\[[^]]*\"127.0.0.1:${BASE}\""
}

node0_still_sees_node2() {
    status_of "$MGMT" | grep -q "\"unreachable\":\[\]"
}

dropped_positive() {
    local d
    d=$(json_num droppedCount "$(curl -s --max-time 5 "http://127.0.0.1:$MGMT/cluster/faults")")
    [[ ${d:-0} -gt 0 ]]
}

peer_reachable_again() {
    local body
    body=$(status_of "$MGMT")
    grep -q "\"unreachable\":\[\]" <<<"$body" || return 1
    grep -q '"converged":true' <<<"$body"
}

echo "==> 启动 $N 个进程（集群端口 $BASE 起，管理端口 $MGMT 起）"
CACTOR_NO_AUTODOWN=1 CACTOR_LOG_LEVEL=error CACTOR_MGMT_PORT=$MGMT "$BIN" "$BASE" "$SEED" "$N" hold >"$logdir/node0.log" 2>&1 &
pids+=($!)
for i in $(seq 1 $((N - 1))); do
    CACTOR_NO_AUTODOWN=1 CACTOR_LOG_LEVEL=error CACTOR_MGMT_PORT=$((MGMT + i)) "$BIN" "$((BASE + i))" "$SEED" "$N" hold >"$logdir/node$i.log" 2>&1 &
    pids+=($!)
done

wait_until "三个节点都收敛为 $N 个 Up 成员" "$WAIT" all_converged

echo "==> 黑洞 node0 -> node2（单向，等价于拔掉一根网线）"
curl -s --max-time 5 "http://127.0.0.1:$MGMT/cluster/blackhole?addr=127.0.0.1:$((BASE + 2))" | sed 's/^/  /'
wait_until "被丢包的一端（node2）把 node0 判为不可达" "$WAIT" node2_reports_node0_unreachable
wait_until "node0 的出站帧确实被丢" "$WAIT" dropped_positive
node0_still_sees_node2 || fail "单向黑洞应只影响一个方向：node0 不应把 node2 判为不可达"

faults=$(curl -s --max-time 5 "http://127.0.0.1:$MGMT/cluster/faults")
grep -q "\"127.0.0.1:$((BASE + 2))\"" <<<"$faults" || fail "faults 未记录黑洞地址：$faults"
dropped=$(json_num droppedCount "$faults")
echo "  ✓ 黑洞期间丢弃 $dropped 帧"

echo "==> 恢复链路"
curl -s --max-time 5 "http://127.0.0.1:$MGMT/cluster/heal?addr=127.0.0.1:$((BASE + 2))" | sed 's/^/  /'
wait_until "链路恢复后重新可达并收敛" "$WAIT" peer_reachable_again
wait_until "三个节点视图都恢复" "$WAIT" all_converged

echo "进程级故障注入验证：通过"
