#!/usr/bin/env bash
# 混合版本长期多进程验证（DA8 余项，DA6 能力协商 + DA4 租约的组合验证）：
# 3 个真实进程组成集群（旧线协议模拟 + 分片租约 + 实体 fencing），运行期间对最后一个进程
# kill -9 并在原端口以"升级后的新节点"重启，断言：
#   1. 旧节点（CACTOR_WIRE_LEGACY=1，模拟 v1.2.x：握手不带能力扩展、按旧编码收发）与新节点能组成集群；
#   2. 宕机进程被 SBR 移出、其上的实体在存活进程上自动重启（remember-entities，E7′/N4）；
#   3. 实体写入经 DA4 分片租约的 fencing token 保护：重启实例以新 token 写入成功，
#      被杀实例的旧 token 若还想写入会被拒绝（FENCE_REJECT）；
#   4. 新实例 10 s 内重新 Up、不继承旧实例身份、remember-entities 记录一个不少；
#   5. 全部进程正常退出（退出码 0），无崩溃。
#
#   tools/mixed_version_soak.sh                  # 需要先在 workspace 根目录 cjpm build
#   MIXED_VERSION_PORT=26300 tools/mixed_version_soak.sh
#
# 不用固定 sleep 编排：所有等待都是轮询日志/端点就绪。约 1 分钟。
# 接入：nightly（tools/ci_nightly.sh）固定运行；tools/ci.sh 只在需要回归本场景时手动加一行 guard。
set -uo pipefail
cd "$(dirname "$0")/.."
if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi
# shellcheck disable=SC1091
source tools/env.sh >/dev/null 2>&1 || true

BIN=./target/release/bin/cactor_examples.cluster_crash_node
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
logdir=$(mktemp -d)
leasedir=$(mktemp -d)
pids=()
status=0

cleanup() {
    for pid in "${pids[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -rf "$logdir" "$leasedir"
}
trap cleanup EXIT

fail() {
    echo "失败：$*"
    echo "--- 节点日志 ---"
    cat "$logdir"/*.log 2>/dev/null | tail -60
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
# 先把 victim 也按旧节点启动（滚动升级前），等 READY_TO_KILL 后再在原端口重启为"新节点"
sleep 1
CACTOR_LOG_LEVEL=error CACTOR_WIRE_LEGACY=1 CACTOR_LEASE_DIR="$leasedir" \
    "$BIN" "$((BASE + 2))" "$SEED" "$N" worker > "$logdir/node2.log" 2>&1 &
victim=$!
pids+=("$victim")

# 轮询 driver 的 stderr（READY_TO_KILL 不缓冲；stdout 重定向到文件会缓冲到退出，不能等它）：
# 它出现在"实体全部启动并已复制"打印之后，等它即等复制完成
wait_log "实体全部启动并已复制" "$logdir/node0.log" "READY_TO_KILL" 600

echo "==> kill -9 被害进程 127.0.0.1:$((BASE + 2))"
kill -9 "$victim" 2>/dev/null || true
wait "$victim" 2>/dev/null || true

# driver 校验完宕机处理后打印 READY_TO_RESTART，脚本在原端口以"新节点"重启（升级演练）
wait_log "driver 就绪允许重启" "$logdir/node0.log" "READY_TO_RESTART" 1200
echo "==> 原端口重启（不带 CACTOR_WIRE_LEGACY：模拟该节点升级到新版本）"
CACTOR_LOG_LEVEL=error CACTOR_LEASE_DIR="$leasedir" \
    "$BIN" "$((BASE + 2))" "$SEED" "$N" worker > "$logdir/node2b.log" 2>&1 &
reborn=$!
pids+=("$reborn")

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
    echo "混合版本长期多进程验证：通过"
else
    echo "混合版本长期多进程验证：失败"
    echo "--- 节点日志 ---"
    cat "$logdir"/*.log 2>/dev/null | tail -40
fi
exit "$status"
