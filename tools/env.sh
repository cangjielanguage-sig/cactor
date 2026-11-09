#!/usr/bin/env bash
# CActor 构建环境：source tools/env.sh
#   1. cjpm 不在 PATH 时 source 仓颉 envsetup.sh（CANGJIE_HOME，默认 ~/.cangjie/1.2.0/cangjie）
#   2. CANGJIE_STDX_PATH 未设置时，指向本机已安装的最新 stdx 静态库目录（~/.cangjie/stdx-*/<平台>_cjnative/static/stdx）
# 根 cjpm.toml 的 bin-dependencies 用 ${CANGJIE_STDX_PATH} 引入 stdx（E65 的 TLS）；未设置时 cjpm 只会报
# "imports package 'stdx...' but it is not added as a dependency"，不直观，所以这里先检查并给出明确提示。
if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi
if [ -z "${CANGJIE_STDX_PATH:-}" ]; then
    case "$(uname -s)-$(uname -m)" in
        Linux-x86_64) _cactor_plat=linux_x86_64_cjnative ;;
        Linux-aarch64) _cactor_plat=linux_aarch64_cjnative ;;
        Darwin-arm64) _cactor_plat=darwin_aarch64_cjnative ;;
        Darwin-x86_64) _cactor_plat=darwin_x86_64_cjnative ;;
        *) _cactor_plat="" ;;
    esac
    _cactor_stdx=""
    if [ -n "$_cactor_plat" ]; then
        # 多个版本时取版本号最大的
        _cactor_stdx=$(ls -d "$HOME"/.cangjie/stdx-*/"$_cactor_plat"/static/stdx 2>/dev/null | sort -V | tail -1 || true)
    fi
    if [ -n "$_cactor_stdx" ]; then
        export CANGJIE_STDX_PATH="$_cactor_stdx"
    else
        echo "找不到 stdx：请安装仓颉 stdx（https://gitcode.com/Cangjie/cangjie_stdx）并设置 CANGJIE_STDX_PATH 为其 static/stdx 目录" >&2
    fi
    unset _cactor_plat _cactor_stdx
fi
