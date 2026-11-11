#!/usr/bin/env bash
# 发布 CActor 到仓颉中心仓（pkg.cangjie-lang.cn）。
#
# 为什么不能直接 `cjpm publish`：
#   1. cjpm 不允许发布 workspace（`cannot publish a workspace to central repository`），
#      而本仓库 14 个模块同处一个 workspace；
#   2. 发布产物里的依赖必须是中心仓版本号（`dependency 'x' from field 'dependencies'
#      should be in format of centray repository while bundling`），仓库里却是 path 依赖；
#   3. 每个 artifact 根目录必须有 README.md。
# 因此本脚本把模块拷到 workspace 之外的暂存目录，把 path 依赖改写为版本依赖后再发布。
#
# 关键：暂存 manifest **不能**带 [replace]。cjpm 会把 cjpm.toml 一起打进 artifact，
# 一旦注入 `[replace] x = { path = "/绝对/路径" }`，就会把发布机的路径泄漏给所有使用者，
# 在没有该路径的机器上直接构建失败：
#   `Error: the path '/...' of module 'x' does not exist which is listed in 'replace' field`
# 所以暂存目录里的依赖一律从中心仓解析——这也顺带验证了真实消费者的构建体验。
#
# 用法：
#   tools/publish.sh               # 按依赖顺序发布全部 12 个库模块
#   tools/publish.sh core runtime  # 只发布指定模块（顺序按模块表自动纠正）
#   STAGE=/path tools/publish.sh   # 指定暂存目录（默认新建临时目录）
#   TESTS=1 tools/publish.sh       # 发布前跑一遍模块自身测试（默认跳过）
#   LINT=1  tools/publish.sh       # 发布前跑 cjlint（默认跳过，见下）
#
# 为什么默认 --skip-lint：cjpm 的 lint 环节用本机 cjlint 默认规则集，
# G.FUN.02（未使用参数）等 MANDATORY 规则会把大量“接口实现桩”判为错误，
# 发布时直接以 'code specifications with level MANDATORY are violated' 失败；
# 代码风格问题另有 tools/ci.sh 把关。需要严格把关时用 LINT=1。
# 注意：cjpm publish 会复用已存在的 target/*.cjp 与 meta-data.json，
# 所以先显式 bundle 再 publish，否则 publish 会自己跑一遍完整 bundle（含 lint 与测试）。
#
# 前置条件：
#   - 全局 token 已写入 $CANGJIE_HOME/tools/config/cangjie-repo.toml 的 [repository.home]
#   - 依赖模块的**同版本**已按顺序发布成功（暂存目录按版本号从中心仓解析依赖，
#     发布 1.2.1 之前必须先把 foundation 1.2.1 发出去，这也是顺序不可打乱的原因）
#   - 在 workspace 根目录执行；已 source tools/env.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

REGISTRY=${REGISTRY:-https://pkg.cangjie-lang.cn/registry}

# 依赖自底向上，顺序即发布顺序（见 tools/check_module_deps.py）
MODULES=(foundation macros core runtime cactor patterns testkit streams remote persistence cluster xstate)

pkg_version() {
    sed -n 's/^version = "\([^"]*\)".*/\1/p' "modules/$1/cjpm.toml" | head -1
}

VERSION=${VERSION:-$(pkg_version foundation)}
STAGE=${STAGE:-$(mktemp -d "${TMPDIR:-/tmp}/cactor-publish.XXXXXX")}

want=("$@")
selected() {
    [ ${#want[@]} -eq 0 ] && return 0
    local x
    for x in "${want[@]}"; do [ "$x" = "$1" ] && return 0; done
    return 1
}

# cjpm 会把索引缓存在 ~/.cjpm/repository/index/<org>/<2>/<2>/<name>，且一旦缓存就不再回源；
# 而注册中心的索引/其 CDN 边缘有生成与缓存延迟，旧内容里没有新版本行，解析就会报
# `no available version for module 'x' by require '1.2.1'`。因此每次构建前都清一遍。
clear_index_cache() {
    local m dname
    for m in "${MODULES[@]}"; do
        dname="cactor_$m"
        rm -f "$HOME"/.cjpm/repository/index/*/"${dname:0:2}/${dname:2:2}/$dname" 2>/dev/null || true
    done
}

clear_index_cache

# 索引是异步生成的，发布后立刻消费会 404；等**该版本**出现在索引里再放行下一个模块。
# 注意不能只看 HTTP 200：模块的索引文件早就有旧版本了，新版本行是稍后追加的。
wait_index() {
    local name=$1 ver=$2 url i
    url="$REGISTRY/index/${name:0:2}/${name:2:2}/$name"
    for i in $(seq 1 120); do
        if curl -sL "$url" 2>/dev/null | grep -qF "\"version\":\"$ver\""; then
            clear_index_cache
            return 0
        fi
        sleep 5
    done
    echo "!! ${name} ${ver} 10 分钟内未进入索引: ${url}" >&2
    return 1
}

if [ -z "${CANGJIE_STDX_PATH:-}" ]; then
    echo "!! CANGJIE_STDX_PATH 未设置，remote/cluster 会构建失败（先 source tools/env.sh）" >&2
    exit 1
fi

# 需要 stdx 的模块：remote 直接 import stdx.*，cluster 依赖 remote
STDX_MODULES=" remote cluster "

failed=()
for m in "${MODULES[@]}"; do
    selected "$m" || continue
    name="cactor_$m"
    dir="$STAGE/$name"
    echo "=== 发布 $name ($VERSION)"

    rm -rf "$dir"
    mkdir -p "$STAGE"
    rsync -a --exclude target --exclude .cjpm --exclude cjpm.lock "modules/$m/" "$dir/"

    # path 依赖 -> 中心仓版本依赖；不带 [replace]，避免把绝对路径打进 artifact
    python3 - "$dir/cjpm.toml" "$ROOT" <<'PY'
import re, sys, os
path, root = sys.argv[1], sys.argv[2]

def version_of(mod):
    text = open(os.path.join(root, 'modules', mod, 'cjpm.toml'), encoding='utf-8').read()
    m = re.search(r'(?m)^version = "([^"]+)"', text)
    return m.group(1) if m else ''

s = open(path, encoding='utf-8').read()
def fix(m):
    name, body = m.group(1), m.group(2)
    pm = re.search(r'path\s*=\s*"([^"]+)"', body)
    if not pm:
        return m.group(0)                      # 非 path 依赖原样保留
    mod = os.path.basename(pm.group(1).rstrip('/'))
    ver = version_of(mod)
    if not ver:
        sys.exit(f'cannot resolve version for dependency {name} (module {mod})')
    return f'{name} = "{ver}"'
s = re.sub(r'(?m)^(\w+)\s*=\s*\{([^}]*)\}$', fix, s)
open(path, 'w', encoding='utf-8').write(s)
PY

    # remote/cluster 直接或间接 import stdx.*：workspace 里靠根 cjpm.toml 的
    # [target.*.bin-dependencies] path-option 提供 stdx 静态库，脱离 workspace 后
    # 模块必须自己声明，否则 bundle 报
    # `root package 'cactor_remote' imports package 'stdx.crypto.common' ... but it is not added as a dependency`。
    case "$STDX_MODULES" in *" $m "*)
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
        ;;
    esac

    if [ ! -f "$dir/README.md" ] && [ ! -f "$dir/README_zh.md" ]; then
        echo "!! $name 缺少 README.md，cjpm bundle 会失败" >&2
        failed+=("$m"); continue
    fi

    # 守卫：artifact 会带上 cjpm.toml，绝不能含 [replace] 或绝对路径
    if grep -qE '^\[replace\]|"/' "$dir/cjpm.toml"; then
        echo "!! $name 暂存 cjpm.toml 含 [replace] 或绝对路径，拒绝发布：" >&2
        grep -nE '^\[replace\]|"/' "$dir/cjpm.toml" >&2
        failed+=("$m"); continue
    fi

    bundle_args=(--skip-test --skip-lint)
    [ "${TESTS:-0}" = "1" ] && bundle_args=(--skip-lint)
    [ "${LINT:-0}" = "1" ] && bundle_args=("--skip-test")
    if [ "${TESTS:-0}" = "1" ] && [ "${LINT:-0}" = "1" ]; then bundle_args=(); fi

    # publish 会复用 bundle 生成的 target/*.cjp，因此这里先 bundle 再 publish。
    # 索引存在生成/缓存延迟，首轮可能拿不到刚发布的依赖版本，清缓存重试几次。
    bundle_ok=0
    for attempt in 1 2 3; do
        clear_index_cache
        if ( cd "$dir" && cjpm bundle "${bundle_args[@]}" ) > "$dir.bundle.log" 2>&1; then
            bundle_ok=1
            break
        fi
        echo "-- $name bundle 第 $attempt 次失败，清理索引缓存后重试（日志 $dir.bundle.log）" >&2
        tail -4 "$dir.bundle.log" >&2
        sleep 30
    done
    if [ "$bundle_ok" = "0" ]; then
        echo "!! $name bundle 失败（日志 $dir.bundle.log）" >&2
        tail -20 "$dir.bundle.log" >&2
        failed+=("$m"); continue
    fi
    if ! ( cd "$dir" && cjpm publish ); then
        echo "!! $name 发布失败" >&2
        failed+=("$m"); continue
    fi
    if ! wait_index "$name" "$VERSION"; then
        failed+=("$m"); continue
    fi
    echo "--- $name $VERSION 已可被消费方解析"
done

if [ ${#failed[@]} -gt 0 ]; then
    echo "失败模块: ${failed[*]}" >&2
    exit 1
fi
echo "全部发布完成。"
