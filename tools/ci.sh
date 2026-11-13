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
guard() { timeout --foreground "$STEP_TIMEOUT" "$@"; }

step "构建（workspace，-O2）"
cjpm build

step "模块分层检查"
python3 tools/check_module_deps.py

step "公开 API 快照检查（E51）"
# 签名有未登记的变化即失败；有意修改时 python3 tools/api_snapshot.py --update 并在 CHANGELOG 记录
python3 tools/api_snapshot.py

step "宏公开面检查（A5）"
bash tools/check_macro_surface.sh

step "DSL 行数对照（A1）"
bash tools/dsl_line_count.sh

step "宏负例编译测试（A2）"
bash tools/negative_tests.sh

step "全部测试"
log=$(mktemp)
# --parallel 1：仓库里的集群/分片用例会起真实 TCP 集群并等待选主/故障检测；
# 在本机（桌面负载 40–55）上并行跑多个集群用例时，故障检测偶发把节点误判为不可达，
# 出现 `distinct.size==2` / `localEntityCount()==0` 这类分布断言失败（隔离单跑 10/10 通过）。
# 串行执行的墙钟时间与并行基本相同（约 2m50s，长等待本来就占主导），因此默认串行。
# env -u CACTOR_LOG_LEVEL：调用方若导出了日志级别（例如 error），日志类用例的 warn 输出会被过滤而误报失败（E26）。
# --timeout-each：单个用例超时即判失败，死循环不再卡死门禁（E26）。
# --no-progress：输出写进文件，交互式进度条没有意义；且仓颉 1.2.0 的 std.unittest 进度汇报线程偶发
# NoneValueException（TestProgressCollector.updateBody），全部用例通过时也让 cjpm test 判失败（第 5 批实测：554/554 通过、门禁仍红）。
if ! env -u CACTOR_LOG_LEVEL cjpm test --parallel 1 --no-progress --timeout-each="$TEST_TIMEOUT" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | tee "$log" | grep -E '^Summary|PASSED:|FAILED:|ERROR:' | tail -4; then
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
# 类型化持久化：三个系统先后共用一个目录，64 个购物车并发写入后恢复一件不少、结账快照、删除不回退（E44）
CACTOR_LOG_LEVEL=error guard ./target/release/bin/cactor_examples.typed_persistence_demo | tail -1
guard bash tools/reminder_demo.sh | tail -3
# 分布式数据：3 节点并发读写 + 四种 GC 配置各一轮，无崩溃、全部收敛（E43）
guard tools/ddata_stress.sh 4 5 | tail -1
# 20 秒稳定性自检：收发零丢失、故障全部重启、子 Actor 全部停止、静止存活堆不增长
CACTOR_LOG_LEVEL=off guard ./target/release/bin/cactor_examples.soak_test 20 | tail -5

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
    bash tools/bench_gate.sh
fi

step "全部通过"
