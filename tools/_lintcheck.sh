#!/usr/bin/env bash
# dry lint check
#
# 为什么不能直接 `cjpm bundle --skip-test`：
#   1. cjpm 不允许发布 workspace（cannot publish a workspace to central repository），
#      而本仓库 14 个模块同处一个 workspace；
#   2. 发布产物里的依赖必须是中心仓版本号（`Error: dependency 'x' ... should be in
#      format of centray repository while bundling`），仓库里却是 path 依赖；
#   3. 每个 artifact 根目录必须有 README.md。
# 因此本脚本把模块拷到 workspace 之外的暂存目录，把 path 依赖改写为版本依赖，
# 再用 [replace] 指回本地源码：构建用的是当前源码，而发布元数据记录的是版本号。
#
# 用法：
#   tools/publish.sh               # 按依赖顺序发布全部 12 个库模块
#   tools/publish.sh core runtime  # 只发布指定模块（顺序按模块表自动纠正）
#   STAGE=/path tools/publish.sh   # 指定暂存目录（默认新建临时目录）
#
# 前置条件：
#   - 全局 token 已写入 $CANGJIE_HOME/tools/config/cangjie-repo.toml 的 [repository.home]
#   - 依赖模块已按顺序发布成功（消费者解析的是中心仓版本号）
#   - 在 workspace 根目录执行；已 source tools/env.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

# 依赖自底向上，顺序即发布顺序（见 check_module_deps.py）
MODULES=(foundation macros core runtime cactor patterns testkit streams remote persistence cluster xstate)

VERSION=$(sed -n 's/^version = "\([^"]*\)".*/\1/p' modules/foundation/cjpm.toml | head -1)
STAGE=${STAGE:-$(mktemp -d "${TMPDIR:-/tmp}/cactor-publish.XXXXXX")}

want=("$@")
selected() {
    [ ${#want[@]} -eq 0 ] && return 0
    local x
    for x in "${want[@]}"; do [ "$x" = "$1" ] && return 0; done
    return 1
}

echo "版本: $VERSION"
echo "暂存目录: $STAGE"
if [ -z "${CANGJIE_STDX_PATH:-}" ]; then
    echo "!! CANGJIE_STDX_PATH 未设置，remote/cluster 会构建失败（先 source tools/env.sh）" >&2
    exit 1
fi

failed=()
for m in "${MODULES[@]}"; do
    selected "$m" || continue
    dir="$STAGE/cactor_$m"
    echo "=== 发布 cactor_$m ($VERSION)"

    rm -rf "$dir"
    mkdir -p "$STAGE"
    rsync -a --exclude target --exclude .cjpm --exclude cjpm.lock "modules/$m/" "$dir/"

    # path 依赖 -> 版本依赖 + [replace] 指回本地源码
    python3 - "$dir/cjpm.toml" "$ROOT" "$VERSION" <<'PY'
import re, sys
path, root, ver = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(path, encoding='utf-8').read()
replaces = []
def fix(m):
    name, body = m.group(1), m.group(2)
    pm = re.search(r'path\s*=\s*"([^"]+)"', body)
    if not pm:
        return m.group(0)          # 非 path 依赖原样保留
    replaces.append((name, pm.group(1)))
    return f'{name} = "{ver}"'
s = re.sub(r'(?m)^(\w+)\s*=\s*\{([^}]*)\}$', fix, s)
if replaces:
    s = s.rstrip() + '\n\n[replace]\n'
    for name, rel in replaces:
        s += f'{name} = {{ path = "{root}/modules/{rel.split("/")[-1]}" }}\n'
open(path, 'w', encoding='utf-8').write(s)
PY

    # remote 无条件 import stdx.*，而 workspace 里是根 cjpm.toml 的
    # [target.*.bin-dependencies] path-option 提供 stdx 静态库路径；脱离 workspace 后
    # 单个模块必须自己声明，否则 bundle 报 "imports package 'stdx.crypto.common' ...
    # but it is not added as a dependency"。这些 target 段不会进入发布元数据。
    python3 - "$dir/cjpm.toml" <<'PY_STDX'
import sys
p = sys.argv[1]
s = open(p, encoding='utf-8').read()
if 'bin-dependencies' not in s:
    s = s.rstrip() + '''

[target.x86_64-unknown-linux-gnu.bin-dependencies]
  path-option = ["${CANGJIE_STDX_PATH}"]

[target.aarch64-unknown-linux-gnu.bin-dependencies]
  path-option = ["${CANGJIE_STDX_PATH}"]

[target.aarch64-apple-darwin.bin-dependencies]
  path-option = ["${CANGJIE_STDX_PATH}"]

[target.x86_64-apple-darwin.bin-dependencies]
  path-option = ["${CANGJIE_STDX_PATH}"]
'''
    open(p, 'w', encoding='utf-8').write(s)
PY_STDX

    if [ ! -f "$dir/README.md" ] && [ ! -f "$dir/README_zh.md" ]; then
        echo "!! cactor_$m 缺少 README.md，cjpm bundle 会失败" >&2
        failed+=("$m"); continue
    fi

    if ! ( cd "$dir" && cjpm bundle --skip-test ); then
        echo "!! cactor_$m 发布失败" >&2
        failed+=("$m")
    fi
done

if [ ${#failed[@]} -gt 0 ]; then
    echo "失败模块: ${failed[*]}" >&2
    exit 1
fi
echo "全部发布完成。"
