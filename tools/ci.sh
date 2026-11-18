#!/usr/bin/env bash
# CActor CI 门禁：构建、分层检查、全部测试、可选基准
#
#   tools/ci.sh                    # build + 分层检查 + 全部测试 + 示例自检
#   tools/ci.sh --bench            # 另外运行基准门禁（需要空闲机器，数字才有意义）
#   tools/ci.sh --soak [秒]         # 另外运行长时间稳定性测试（默认 600 秒）
#   tools/ci.sh --soak 300 --bench # 参数可组合、顺序任意
#
# 环境变量：
#   CI_TEST_TIMEOUT   单个测试用例的超时（默认 120s）。任何用例死循环 / 死锁都会在该时限内判失败，
#                     不再把整个门禁卡死（E26：6183072 的 Random 路由死循环曾让 ci.sh 永不结束）。
#   CI_STEP_TIMEOUT   每个示例 / 演示脚本的超时秒数（默认 300）。
#
# 注意：必须在 workspace 根目录执行 cjpm build；`cjpm build -m <模块>` 不继承 workspace 的 -O2，
# 用它构建出的基准程序会慢 2–3 倍。
set -euo pipefail

cd "$(dirname "$0")/.."

# 仓颉环境 + stdx 路径（E65：根 cjpm.toml 以 ${CANGJIE_STDX_PATH} 引入 stdx）
# shellcheck disable=SC1091
source tools/env.sh

step() { printf '\n==> %s\n' "$*"; }

# ---- 参数：循环解析，--bench 与 --soak 可同时使用 ----
RUN_BENCH=0
SOAK_SECS=""
while [ $# -gt 0 ]; do
    case "$1" in
        --bench) RUN_BENCH=1; shift ;;
        --soak)
            shift
            if [ $# -gt 0 ] && [[ "$1" =~ ^[0-9]+$ ]]; then SOAK_SECS="$1"; shift; else SOAK_SECS=600; fi ;;
        *) echo "未知参数: $1"; exit 2 ;;
    esac
done

TEST_TIMEOUT="${CI_TEST_TIMEOUT:-120s}"
STEP_TIMEOUT="${CI_STEP_TIMEOUT:-300}"
# 示例 / 脚本统一加超时：挂死即失败（timeout 退出码 124），而不是让门禁无限等待
guard() {
    # macOS 的 /usr/bin/env shebang 会清掉 DYLD_*，让脚本启动的仓颉节点找不到运行时。
    # 显式用当前 PATH 中的 Bash 执行 shell 步骤，保留 env.sh 已准备的动态库路径。
    if [[ "$1" == *.sh ]]; then
        # 用当前解释器（$BASH）而不是 PATH 里的 bash：macOS 自带 /bin/bash 3.2，
        # 而 ci.sh 可能由 /opt/homebrew/bin/bash 5.x 启动；子脚本若依赖 4+ 内建
        # （mapfile/readarray/关联数组）会在 3.2 下报“未绑定变量”把门禁判红。
        timeout --foreground "$STEP_TIMEOUT" "${BASH:-bash}" "$@"
    else
        timeout --foreground "$STEP_TIMEOUT" "$@"
    fi
}

step "构建（workspace，-O2）"
cjpm build

step "模块分层检查"
python3 tools/check_module_deps.py

step "公开 API 快照检查（E51）"
# 签名有未登记的变化即失败；有意修改时 python3 tools/api_snapshot.py --update 并在 CHANGELOG 记录
python3 tools/api_snapshot.py

step "@Deprecated 完整性检查（E51：每个弃用都必须有 since 和移除版本）"
python3 tools/check_deprecations.py

step "宏公开面检查（A5）"
"${BASH:-bash}" tools/check_macro_surface.sh

step "DSL 行数对照（A1）"
"${BASH:-bash}" tools/dsl_line_count.sh

step "宏负例编译测试（A2）"
"${BASH:-bash}" tools/negative_tests.sh

step "全部测试"
log=$(mktemp)
# --parallel 1：仓库里的集群/分片用例会起真实 TCP 集群并等待选主/故障检测；
# 在本机（桌面负载 40–55）上并行跑多个集群用例时，故障检测偶发把节点误判为不可达，
# 出现 `distinct.size==2` / `localEntityCount()==0` 这类分布断言失败（隔离单跑 10/10 通过）。
# 串行执行的墙钟时间与并行基本相同（约 2m50s，长等待本来就占主导），因此默认串行。
# CACTOR_LOG_LEVEL：调用方若导出了日志级别（例如 error），日志类用例的 warn 输出会被过滤而误报失败（E26）。
# 用「子 shell 里 unset」而不是 `env -u CACTOR_LOG_LEVEL cjpm ...`：
# `/usr/bin/env`、`/bin/bash`、`/usr/bin/timeout` 都是 dyld 共享缓存里的平台二进制，
# macOS 在执行平台二进制时会把环境里的 DYLD_* 全部剔除（SIP）。所以 `env -u X cjpm` 不仅删掉 X，
# 连 tools/env.sh 刚准备的 DYLD_LIBRARY_PATH / DYLD_FALLBACK_LIBRARY_PATH 也一起丢掉，
# 而 `cjpm test` 起的测试进程找不到仓颉运行时与 Homebrew 的 OpenSSL 3，
# 落到 /usr/lib/libcrypto.dylib（LibreSSL 桩）→ SIGABRT（TLS 用例 7 例 ERROR，2026-11-16 实测定为根因）。
# 子 shell 的 unset 不 exec 新程序，环境原样传给 cjpm。
# --timeout-each：单个用例超时即判失败，死循环不再卡死门禁（E26）。
# --no-progress：输出写进文件，交互式进度条没有意义；且仓颉 1.2.0 的 std.unittest 进度汇报线程偶发
# NoneValueException（TestProgressCollector.updateBody），全部用例通过时也让 cjpm test 判失败（第 5 批实测：554/554 通过、门禁仍红）。
if ! ( unset CACTOR_LOG_LEVEL; cjpm test --parallel 1 --no-progress --timeout-each="$TEST_TIMEOUT" 2>&1 ) | sed 's/\x1b\[[0-9;]*m//g' | tee "$log" | grep -E '^Summary|PASSED:|FAILED:|ERROR:' | tail -4; then
    true
fi
if grep -qE '\[ (FAILED|ERROR) \] CASE' "$log" || ! grep -q 'cjpm test success' "$log"; then
    echo "测试失败："
    grep -E '\[ (FAILED|ERROR) \] CASE' "$log" | sort -u || true
    exit 1
fi

step "并行集群套件（E43：集群用例 8 进程并行跑 2 轮，崩溃与断言失败都判红）"
# 上面的全量测试为了稳定串行执行；集群套件另外并行跑，保留"并行下零 SIGSEGV"的回归防线（N0）。
# 50 轮验收：tools/cluster_parallel_soak.sh 50 8
guard tools/cluster_parallel_soak.sh 2 8 | tail -3

step "示例自检"
CACTOR_LOG_LEVEL=error guard ./target/release/bin/cactor_examples.chat_room
CACTOR_LOG_LEVEL=error cjHeapSize=4GB guard ./target/release/bin/cactor_examples.scale_test 50000 child | tail -3
guard tools/cluster_demo.sh | tail -2
# 同一多进程集群全部走 TLS 1.3 双向认证（E65）：每个进程都只接受本集群 CA 签发的证书
guard tools/cluster_demo.sh --tls | grep -E "TLS 连接|验证|正常退出"
# 只显示结论行；但任何一项判定为 false、有重复启动或进程失败时把那一行也显示出来，失败原因不被过滤掉
guard tools/cluster_crash_demo.sh | grep -E "验证|重新加入|记录完整|正常退出|：false|重复启动：[1-9]|重复启动 t-|有 0 个实体|有进程失败|没有就绪"
# 进程级故障注入（E62）：3 个真进程，黑洞一条链路 → 对端判不可达 → 恢复后重新收敛
guard tools/cluster_chaos.sh | grep -E "✓|通过|失败"
# 持久化日志：随机 kill -9 + 尾部零填充 / 写到一半，已确认的事件一条不少（E8′）
guard tools/journal_crash_demo.sh | tail -2
# 跨进程文件租约（DA4）：两个进程争抢、kill -9 持有者、ttl 后接手且 token 更大、旧 token 写入被 fencing 拒绝（约 3 s）
guard tools/lease_contend_demo.sh | tail -1
# 混合版本多进程验证（DA8 / DA6 / DA4 组合）：旧线协议模拟 + 租约 + kill -9 + 原端口重启（约 1 min）
guard tools/mixed_version_soak.sh | tail -1
# 类型化持久化：三个系统先后共用一个目录，64 个购物车并发写入后恢复一件不少、结账快照、删除不回退（E44）
CACTOR_LOG_LEVEL=error guard ./target/release/bin/cactor_examples.typed_persistence_demo | tail -1
guard "${BASH:-bash}" tools/reminder_demo.sh | tail -3
# E61：无需外部凭据的真实 TCP SSE，以及会话磁盘恢复 / 重试 / 取消 / 流式进度
CACTOR_LOG_LEVEL=error guard ./target/release/bin/cactor_examples.llm_http_demo
CACTOR_LOG_LEVEL=off guard ./target/release/bin/cactor_examples.agent_orchestration_demo
# 分布式数据：3 节点并发读写 + 四种 GC 配置各一轮，无崩溃、全部收敛（E43）
guard tools/ddata_stress.sh 4 5 | tail -1
# 20 秒稳定性自检：收发零丢失、故障全部重启、子 Actor 全部停止、静止存活堆不增长
CACTOR_LOG_LEVEL=off guard ./target/release/bin/cactor_examples.soak_test 20 | tail -5

# 纯进程示例自检（E54）：这批示例的 main 自带断言，通过打印「xx：通过」并以 0 退出，
# 失败则非 0。门禁不再只跑十几个，避免"示例腐烂但 CI 仍绿"。
# 只打印最后一行（各示例自检的结论行），失败时 guard 的退出码先让 set -e 终止。
# 清单写成完整二进制名（cactor_examples.<示例名>），tools/check_examples.py 直接扫描本文件
# 判定"哪些示例进了门禁"，避免再维护第二份会漂移的列表。
EXAMPLES_SELF_CHECK="
cactor_examples.best01_hello_typed cactor_examples.best02_ask_roundtrip
cactor_examples.best03_supervision_tree cactor_examples.best04_state_machine
cactor_examples.best05_backpressure cactor_examples.best06_receptionist_pool
cactor_examples.best07_event_sourced_account cactor_examples.best08_remote_nodes
cactor_examples.best09_cluster_sharding cactor_examples.best10_agent_workflow
cactor_examples.dsl_showcase cactor_examples.dsl_state_machine cactor_examples.dsl_agent
cactor_examples.streams_demo cactor_examples.typed_actor_demo cactor_examples.hello_world_demo
cactor_examples.ask_demo cactor_examples.ask_pattern_demo cactor_examples.test_framework_demo
cactor_examples.functional_actor_demo cactor_examples.cangjie_features_demo
cactor_examples.cangjie_dsl_features cactor_examples.xstate_demo
cactor_examples.advanced_mailbox_examples cactor_examples.advanced_dispatcher_examples
cactor_examples.enhanced_typed_actor_demo cactor_examples.configuration_usage_test cactor_examples.objectpool_performance_comparison
cactor_examples.simple_objectpool_test cactor_examples.stdlib_objectpool_test
cactor_examples.llm_token_stream cactor_examples.monitoring_demo cactor_examples.persistence_demo
cactor_examples.ex01_agent_runtime cactor_examples.ex02_tenant_orders
cactor_examples.ex03_event_backbone cactor_examples.ex04_edge_config
cactor_examples.ex05_durable_workflow cactor_examples.ex06_http_actor_api
cactor_examples.ex07_cluster_pubsub_chat cactor_examples.ex08_cluster_singleton_job
cactor_examples.ex09_grains_cart cactor_examples.ex10_fsm_order
cactor_examples.ex11_router_pool cactor_examples.ex12_streams_sse
cactor_examples.ex13_projection_read_model cactor_examples.ex14_circuit_breaker
cactor_examples.ex15_work_queue cactor_examples.ex16_persistence_upcast
cactor_examples.ex17_remote_watch cactor_examples.ex18_durable_pubsub
cactor_examples.ex19_cluster_router cactor_examples.ex20_singleton_lease
cactor_examples.ex21_read_min_cap cactor_examples.ex22_grains_async
cactor_examples.ex23_sharding_passivation cactor_examples.ex24_at_least_once
cactor_examples.ex25_broadcast_hub
"
for ex in $EXAMPLES_SELF_CHECK; do
    CACTOR_LOG_LEVEL=error guard "./target/release/bin/${ex}" | tail -1
done

# 示例治理（E54）：每个目录必须在 ci.sh 运行或在白名单里写明理由，README 清单与目录一致
python3 tools/check_examples.py

step "库代码中不应新增 println（报告类 print*/display*/show* 函数除外）"
python3 - <<'EOF'
import os, re, sys
FUNC = re.compile(r'^\s*(?:public |private |protected |internal |static |open |override |redef |operator )*func\s+(\w+)')
KEEP = re.compile(r'^(print|display|show|dump)', re.I)
libs = ['foundation', 'core', 'runtime', 'patterns', 'remote', 'persistence', 'cluster', 'streams', 'xstate', 'testkit', 'cactor']
bad = []
for m in libs:
    for dp, _, fs in os.walk(os.path.join('modules', m, 'src')):
        for f in fs:
            if not f.endswith('.cj') or f.endswith('_test.cj'):
                continue
            cur = None
            for n, line in enumerate(open(os.path.join(dp, f), encoding='utf-8'), 1):
                fm = FUNC.match(line)
                if fm:
                    cur = fm.group(1)
                if re.search(r'(?<![\w.])println\(', line) and not line.strip().startswith('//') and not (cur and KEEP.match(cur)):
                    bad.append(f'{dp}/{f}:{n}')
if bad:
    print('发现 println：'); print('\n'.join(bad[:50])); sys.exit(1)
print('通过')
EOF

if [ -n "$SOAK_SECS" ]; then
    step "长时间稳定性测试（${SOAK_SECS} 秒）"
    CACTOR_LOG_LEVEL=off timeout --foreground "$((SOAK_SECS + 120))" ./target/release/bin/cactor_examples.soak_test "$SOAK_SECS"
fi

if [ "$RUN_BENCH" = "1" ]; then
    step "基准门禁（bench_gate.sh 决定退出码）"
    # 2026-11-06 修复（E4）：--bench 接入 bench_gate.sh 门禁，由它决定 CI 退出码；
    # 此前只打印数字不判定，门禁形同虚设。
    "${BASH:-bash}" tools/bench_gate.sh
fi

step "全部通过"
