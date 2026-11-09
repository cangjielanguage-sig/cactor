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

# macOS：target/release/bin 与 unittest_bin 下的可执行文件没有写 rpath，直接运行时仓颉运行时库
# 找不到（dyld: Library not loaded @rpath/libcangjie-runtime.dylib）。cjpm 自己的 shim 也是用
# DYLD_*_LIBRARY_PATH 补齐的；tools/cluster_parallel_soak.sh 等脚本直接跑二进制，
# 统一在这里补上，否则门禁会以 "Abort trap: 6" 退在启动阶段而不是真的测试失败。
#
# 注意两条路径都要设：
#   - target/release/bin/* 的引用能被 DYLD_FALLBACK_LIBRARY_PATH 命中；
#   - unittest_bin/* 用 @rpath，dyld 只查 DYLD_LIBRARY_PATH。
# CANGJIE_HOME 未设置时按仓颉安装的两个常见位置探测（cjpm 在 PATH 里时 env.sh 不会 source envsetup）。
if [ "$(uname -s)" = "Darwin" ]; then
    case "$(uname -m)" in
        arm64) _cactor_arch=aarch64 ;;
        x86_64) _cactor_arch=x64 ;;
        *) _cactor_arch="$(uname -m)" ;;
    esac
    _cactor_cj_home=""
    for _cactor_d in "${CANGJIE_HOME:-}" "${HOME}/cangjie" "${HOME}/.cangjie/1.2.0/cangjie"; do
        if [ -n "$_cactor_d" ] && [ -d "${_cactor_d}/runtime/lib/darwin_${_cactor_arch}_cjnative" ]; then
            _cactor_cj_home="$_cactor_d"
            break
        fi
    done
    if [ -n "$_cactor_cj_home" ]; then
        _cactor_dyld="${_cactor_cj_home}/runtime/lib/darwin_${_cactor_arch}_cjnative:${_cactor_cj_home}/tools/lib"
        export DYLD_FALLBACK_LIBRARY_PATH="${_cactor_dyld}${DYLD_FALLBACK_LIBRARY_PATH:+:${DYLD_FALLBACK_LIBRARY_PATH}}"
        export DYLD_LIBRARY_PATH="${_cactor_dyld}${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"
        unset _cactor_dyld
    fi
    unset _cactor_arch _cactor_cj_home _cactor_d
fi
