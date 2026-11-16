#!/usr/bin/env bash
# 混合版本多进程验证（DA8：DA6 能力协商 + DA4 租约 + 实体 fencing 的组合验证）：
# 3 个真实进程组成集群（旧线协议模拟 + 分片租约 + 实体 fencing），对最后一个进程
# kill -9 并在原端口以"升级后的新节点"重启，断言：
#   1. 旧节点（CACTOR_WIRE_LEGACY=1，模拟 v1.2.x：握手不带能力扩展、按旧编码收发）与新节点能组成集群；
#   2. 宕机进程被 SBR 移出、其上的实体在存活进程上自动重启（remember-entities，E7′/N4）；
#   3. 实体写入经 DA4 分片租约的 fencing token 保护：重启实例以新 token 写入成功，
#      被杀实例的旧 token 若还想写入会被拒绝；
#   4. 新实例 10 s 内重新 Up、不继承旧实例身份、remember-entities 记录一个不少；
#   5. 全部进程正常退出（退出码 0），无崩溃。
#
# 两种模式：
#   短模式（默认，约 1 分钟）：完成上述一次 kill/重启演练。
#   长模式（MIXED_VERSION_DURATION=<秒>）：周期性 kill -9 / 原端口以新节点重启，成员反复移除与加入，
#     旧协议节点（driver / survivor）始终在集群里。每一轮 driver 额外断言：
#       - 已确认的过半写不丢（只增计数器过半写后读回等于已确认次数，且随轮次单调）；
#       - 旧 fencing token 被拒（重启后分片换主、token 前进，driver 用 kill 前的旧 token 重放写入，
#         必须抛 StaleFencingTokenException）；
#       - durable 修订号不回退（实体 preStart 的已确认写入不丢）；
#       - remember-entities 记录完整（= ENTITIES）。
#
#   tools/mixed_version_soak.sh                              # 短模式（需先在 workspace 根目录 cjpm build）
#   MIXED_VERSION_DURATION=600 tools/mixed_version_soak.sh   # 600 s 长压
#   MIXED_VERSION_PORT=26300 tools/mixed_version_soak.sh      # 指定集群起始端口
#
# 所有等待都是轮询日志标记/端点就绪并带超时，不用固定 sleep 编排；任一步失败即非零退出，
# 并保留日志与租约目录（成功时清理）。接入：nightly（tools/ci_nightly.sh）固定运行。
set -uo pipefail
cd "$(dirname "$0")/.."
if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi
# shellcheck disable=SC1091
source tools/env.sh >/dev/null 2>&1 || true

DURATION=${MIXED_VERSION_DURATION:-0}
case "$DURATION" in
    ''|*[!0-9]*) echo "MIXED_VERSION_DURATION 必须是秒数：${DURATION}"; exit 2 ;;
esac
if [ "$DURATION" -gt 0 ]; then
    export MIXED_VERSION_DURATION="$DURATION"
    LONG_MODE=1
else
    unset MIXED_VERSION_DURATION 2>/dev/null || true
    LONG_MODE=0
fi

BIN=./target/release/bin/cactor_examples.cluster_crash_node
if [ ! -x "$BIN" ]; then
    echo "找不到 ${BIN}：请先在 workspace 根目录执行 cjpm build"
    exit 2
fi
# 默认端口可用 MIXED_VERSION_PORT 覆盖；端口冲突时自动前移（跳过被占用的端口段）
MIXED_BASE=${MIXED_VERSION_PORT:-26300}
# 端口冲突自愈：上一轮（或别的脚本）的 TIME_WAIT / 残留进程会让 bind 失败，整组端口前移直到可用
while lsof -nP -iTCP:"${MIXED_BASE}" -sTCP:LISTEN >/dev/null 2>&1 \
    || lsof -nP -iTCP:"$((MIXED_BASE + 2))" -sTCP:LISTEN >/dev/null 2>&1; do
    MIXED_BASE=$((MIXED_BASE + 10))
done
BASE=${MIXED_BASE}
SEED="127.0.0.1:${BASE}"
N=3
VICTIM_PORT=$((BASE + 2))
logdir=$(mktemp -d)
leasedir=$(mktemp -d)
pids=()
status=0
victim_pid=""

cleanup() {
    for pid in "${pids[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    if [ "${status:-0}" -eq 0 ]; then
        rm -rf "$logdir" "$leasedir"
    else
        echo "日志保留：${logdir}（租约目录 ${leasedir}）"
    fi
}
trap cleanup EXIT

fail() {
    echo "失败：$*"
    status=1
    echo "--- 节点日志 ---"
    cat "$logdir"/*.log 2>/dev/null | tail -80
    exit 1
}

wait_log() { # wait_log <描述> <文件> <模式> <次数上限（0.1s 单位）>
    local what=$1 file=$2 pat=$3 tries=$4
    for _ in $(seq 1 "$tries"); do
        if grep -q "$pat" "$file" 2>/dev/null; then
            echo "  ✓ ${what}"
            return 0
        fi
        sleep 0.1
    done
    fail "${what}（等待超时：${pat}）"
}

# 轮询端口进入 LISTEN（替代"启动后固定 sleep 1"，也覆盖慢机器）
wait_listening() { # wait_listening <端口> <次数上限（0.1s 单位）>
    local port=$1 tries=$2
    for _ in $(seq 1 "$tries"); do
        if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    fail "等待端口 ${port} 进入 LISTEN 超时"
}

marker_count() { # 文件里某个标记出现的次数
    local file=$1 pat=$2
    grep -c "$pat" "$file" 2>/dev/null || true
}

# 长压：等待第 expected 次 <标记> 出现。
# 返回 0=已出现；3=driver 已打印 SOAK_DONE（正常结束）；4=driver 报告本轮断言失败；1=超时
wait_marker() { # wait_marker <文件> <标记> <期望次数> <次数上限（0.1s 单位）>
    local file=$1 pat=$2 expected=$3 tries=$4 n
    for _ in $(seq 1 "$tries"); do
        n=$(marker_count "$file" "$pat")
        if [ -n "$n" ] && [ "$n" -ge "$expected" ]; then
            return 0
        fi
        if grep -q "SOAK_CYCLE_FAIL" "$file" 2>/dev/null; then
            return 4
        fi
        if grep -q "SOAK_DONE" "$file" 2>/dev/null; then
            return 3
        fi
        sleep 0.1
    done
    return 1
}

echo "==> 启动 3 个进程（旧线协议模拟 + 分片租约 + fencing；集群端口 ${BASE} 起）"
# driver 与幸存者按 v1.2.x 旧节点行为过线（CACTOR_WIRE_LEGACY=1，见 cluster_crash_node 的说明）；
# 全部节点带 CACTOR_LEASE_DIR：分片托管经 DA4 租约、实体写入带 fencing token
CACTOR_LOG_LEVEL=error CACTOR_WIRE_LEGACY=1 CACTOR_LEASE_DIR="$leasedir" \
    "$BIN" "$BASE" "$SEED" "$N" driver > "$logdir/node0.log" 2>&1 &
driver=$!
pids+=("$driver")
CACTOR_LOG_LEVEL=error CACTOR_WIRE_LEGACY=1 CACTOR_LEASE_DIR="$leasedir" \
    "$BIN" "$((BASE + 1))" "$SEED" "$N" worker > "$logdir/node1.log" 2>&1 &
survivor=$!
pids+=("$survivor")
# 先把 victim 也按旧节点启动（滚动升级前），等 READY_TO_KILL 后再在原端口重启为"新节点"。
# 用端口就绪轮询代替固定等待，确保 driver / survivor 已经在监听后再加入第三个进程
wait_listening "$BASE" 300
wait_listening "$((BASE + 1))" 300
CACTOR_LOG_LEVEL=error CACTOR_WIRE_LEGACY=1 CACTOR_LEASE_DIR="$leasedir" \
    "$BIN" "$VICTIM_PORT" "$SEED" "$N" worker > "$logdir/node2.log" 2>&1 &
victim_pid=$!
victim=$victim_pid
pids+=("$victim")

start_reborn() { # start_reborn <日志文件>
    CACTOR_LOG_LEVEL=error CACTOR_LEASE_DIR="$leasedir" \
        "$BIN" "$VICTIM_PORT" "$SEED" "$N" worker > "$1" 2>&1 &
    victim_pid=$!
    victim=$victim_pid
    pids+=("$victim_pid")
}

if [ "$LONG_MODE" = "0" ]; then
    # ---------- 短模式：一次 kill / 原端口重启 ----------
    # 轮询 driver 的 stderr（READY_TO_KILL 不缓冲；stdout 重定向到文件会缓冲到退出，不能等它）：
    # 它出现在"实体全部启动并已复制"打印之后，等它即等复制完成
    wait_log "实体全部启动并已复制" "$logdir/node0.log" "READY_TO_KILL" 600

    echo "==> kill -9 被害进程 127.0.0.1:${VICTIM_PORT}"
    kill -9 "$victim" 2>/dev/null || true
    wait "$victim" 2>/dev/null || true

    # driver 校验完宕机处理后打印 READY_TO_RESTART，脚本在原端口以"新节点"重启（升级演练）
    wait_log "driver 就绪允许重启" "$logdir/node0.log" "READY_TO_RESTART" 1200
    echo "==> 原端口重启（不带 CACTOR_WIRE_LEGACY：模拟该节点升级到新版本）"
    start_reborn "$logdir/node2b.log"

    wait "$driver" || status=1
    grep -q "多进程宕机与重启验证：通过" "$logdir/node0.log" || {
        echo "driver 校验未通过："
        grep '：' "$logdir/node0.log" | tail -10
        status=1
    }
    wait "$survivor" || status=1

    # fencing 证据：被杀实例残留的旧 token 写入应被拒绝（FENCE_REJECT 出现在任意节点日志）
    fence_rejects=$(grep -h FENCE_REJECT "$logdir"/*.log 2>/dev/null | wc -l | tr -d ' ')
    echo "  ✓ fencing 拒绝旧 token 写入 ${fence_rejects} 次（0 次也正常：仅当被杀进程恰有写入在途）"
else
    # ---------- 长模式：周期性 kill -9 / 原端口重启 ----------
    echo "==> 长压模式：持续 ${DURATION}s（每轮 kill -9 后原端口以新节点重启；旧协议节点常驻）"
    kill_marks=0
    restart_marks=0
    cycle_marks=0
    driver_failed=0
    while :; do
        rc=0
        wait_marker "$logdir/node0.log" "READY_TO_KILL" $((kill_marks + 1)) 1200 || rc=$?
        if [ "$rc" -ne 0 ]; then
            if [ "$rc" -eq 3 ]; then
                break
            fi
            if [ "$rc" -eq 4 ]; then
                driver_failed=1
                break
            fi
            fail "等待 READY_TO_KILL 超时（driver 未继续压测）"
        fi
        kill_marks=$((kill_marks + 1))
        echo "==> [轮 ${kill_marks}] kill -9 127.0.0.1:${VICTIM_PORT}"
        kill -9 "$victim_pid" 2>/dev/null || true
        wait "$victim_pid" 2>/dev/null || true

        rc=0
        wait_marker "$logdir/node0.log" "READY_TO_RESTART" $((restart_marks + 1)) 1800 || rc=$?
        if [ "$rc" -ne 0 ]; then
            if [ "$rc" -eq 4 ]; then
                driver_failed=1
                break
            fi
            fail "等待 READY_TO_RESTART 超时（第 ${kill_marks} 轮）"
        fi
        restart_marks=$((restart_marks + 1))
        echo "==> [轮 ${kill_marks}] 原端口以新节点重启（升级演练）"
        start_reborn "$logdir/node2-cycle${kill_marks}.log"

        rc=0
        wait_marker "$logdir/node0.log" "SOAK_CYCLE_OK" $((cycle_marks + 1)) 1800 || rc=$?
        if [ "$rc" -eq 4 ]; then
            driver_failed=1
            break
        elif [ "$rc" -ne 0 ]; then
            fail "等待第 $((cycle_marks + 1)) 轮 SOAK_CYCLE_OK 超时"
        fi
        cycle_marks=$((cycle_marks + 1))
        echo "  ✓ 第 ${cycle_marks} 轮通过（已确认写、fencing、durable 修订、remember-entities）"
    done

    # driver 报告本轮断言失败时不要立刻 kill：等它打印 SOAK_DONE 并自己退出，
    # 否则 stdout 缓冲里的失败细节会随 kill -9 丢失（stderr 的失败原因已不缓冲）
    if [ "$driver_failed" = "1" ]; then
        for _ in $(seq 1 600); do
            grep -q "SOAK_DONE" "$logdir/node0.log" 2>/dev/null && break
            sleep 0.1
        done
    fi
    wait "$driver" || status=1
    grep -q "混合版本长期压测：通过" "$logdir/node0.log" || {
        echo "driver 长压校验未通过："
        grep -E '轮|压测|失败' "$logdir/node0.log" | tail -12
        status=1
    }
    # fencing 的确定性证据：driver 每轮用 kill 前的旧 token 重放写入，必须被拒
    fence_rejects=$(grep -o "fencing 拒旧 token [0-9]*" "$logdir/node0.log" | tail -1 | sed 's/.* //' || true)
    if [ -n "$fence_rejects" ] && [ "$fence_rejects" -gt 0 ]; then
        echo "  ✓ fencing 拒旧 token 断言通过 ${fence_rejects} 次（driver 确定性重放）"
    else
        echo "fencing 没有拒绝过旧 token（旧 token 重放未被拒）"
        status=1
    fi
    wait "$survivor" || status=1
    # 最后一个重启实例在 driver 离开后正常退出（无崩溃）
    wait "$victim_pid" || status=1
fi

# 租约证据：分片租约在"节点取得分片托管"时逐个创建。64 个分片分布在不同节点，重启/移交过程中
# 个别分片可能在退出前尚未轮到创建，所以要求"大部分已创建"而不是恰好 64：租约门禁的正确性
# （失租即停、旧 token 被拒）由 driver 校验与单测覆盖，这里只验证租约机制确实在起作用
lease_files=$(ls "$leasedir" 2>/dev/null | grep -c '\.lease$' || true)
echo "  ✓ 分片租约文件 ${lease_files}/64"
if [ "$lease_files" -lt 32 ]; then
    echo "分片租约创建过少（不足一半，可能租约未生效）"
    status=1
fi

if [ "$status" -eq 0 ]; then
    if [ "$LONG_MODE" = "0" ]; then
        echo "混合版本多进程验证：通过"
    else
        echo "混合版本长期压测：通过（${DURATION}s）"
    fi
else
    echo "混合版本多进程验证：失败"
    echo "--- 节点日志 ---"
    cat "$logdir"/*.log 2>/dev/null | tail -60
fi
exit "$status"
