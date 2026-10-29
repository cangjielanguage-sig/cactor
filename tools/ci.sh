#!/usr/bin/env bash
# CActor CI 门禁：构建、分层检查、全部测试、可选基准
#
#   tools/ci.sh            # build + 分层检查 + 全部测试
#   tools/ci.sh --bench    # 另外运行基准（需要空闲机器，数字才有意义）
#
# 注意：必须在 workspace 根目录执行 cjpm build；`cjpm build -m <模块>` 不继承 workspace 的 -O2，
# 用它构建出的基准程序会慢 2–3 倍。
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi

step() { printf '\n==> %s\n' "$*"; }

step "构建（workspace，-O2）"
cjpm build

step "模块分层检查"
python3 tools/check_module_deps.py

step "全部测试"
log=$(mktemp)
if ! cjpm test 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | tee "$log" | grep -E '^Summary|PASSED:|FAILED:|ERROR:' | tail -4; then
    true
fi
if grep -qE '\[ (FAILED|ERROR) \] CASE' "$log" || ! grep -q 'cjpm test success' "$log"; then
    echo "测试失败："
    grep -E '\[ (FAILED|ERROR) \] CASE' "$log" | sort -u || true
    exit 1
fi

step "示例自检"
CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.chat_room
CACTOR_LOG_LEVEL=error cjHeapSize=4GB ./target/release/bin/cactor_examples.scale_test 50000 child | tail -3
tools/cluster_demo.sh | tail -2

step "库代码中不应新增 println（报告类 print*/display*/show* 函数除外）"
python3 - <<'EOF'
import os, re, sys
FUNC = re.compile(r'^\s*(?:public |private |protected |internal |static |open |override |redef |operator )*func\s+(\w+)')
KEEP = re.compile(r'^(print|display|show|dump)', re.I)
libs = ['foundation', 'core', 'runtime', 'patterns', 'remote', 'persistence', 'cluster', 'distribution', 'streams', 'xstate', 'testkit', 'cactor']
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

if [[ "${1:-}" == "--bench" ]]; then
    step "基准"
    CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.real_benchmark
fi

step "全部通过"
