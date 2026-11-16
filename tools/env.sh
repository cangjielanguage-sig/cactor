#!/usr/bin/env bash
# CActor 构建环境：source tools/env.sh
#   1. cjpm 不在 PATH 时 source 仓颉 envsetup.sh（CANGJIE_HOME，默认 ~/.cangjie/1.2.0/cangjie）
#   2. CANGJIE_STDX_PATH 未设置时，指向本机已安装的最新 stdx 静态库目录（~/.cangjie/stdx-*/<平台>_cjnative/static/stdx）
# 根 cjpm.toml 的 bin-dependencies 用 ${CANGJIE_STDX_PATH} 引入 stdx（E65 的 TLS）；未设置时 cjpm 只会报
# "imports package 'stdx...' but it is not added as a dependency"，不直观，所以这里先检查并给出明确提示。
if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    #
    # 仓颉可能没装在 CANGJIE_HOME 默认位置（本机是 ~/cangjie）。与文件末尾的 dyld 探测
    # 用同一组候选，找不到才用默认值让 envsetup.sh 自己报错。否则“source 一个不存在的
    # envsetup.sh”会静默返回 0，靠 set -e 也拦不住，最终报“命令未找到”。
    _cactor_envsetup=""
    for _cactor_d in "${CANGJIE_HOME:-}" "${HOME}/cangjie" "${HOME}/.cangjie/1.2.0/cangjie"; do
        if [ -n "$_cactor_d" ] && [ -f "${_cactor_d}/envsetup.sh" ]; then
            _cactor_envsetup="${_cactor_d}/envsetup.sh"
            break
        fi
    done
    : "${_cactor_envsetup:=${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh}"
    #
    # 关闭 set -u 再 source：仓颉 1.2.0 的 envsetup.sh 用
    #   eval "env_var_value=\${$env_var_name}"
    # 读取变量（_remove_cangjie_paths），在 nounset 下遇到未设置的
    # DYLD_LIBRARY_PATH / LD_LIBRARY_PATH 会直接报“未绑定的变量”并中止。
    # tools/ci.sh 在 set -euo pipefail 下 source 本文件，所以这里必须自己兜住，
    # 否则干净 shell 里跑门禁会在第一步就退出（2026-11-16 实测）。
    _cactor_had_nounset=0
    case "$-$" in *u*) _cactor_had_nounset=1 ;; esac
    set +u
    source "$_cactor_envsetup" >/dev/null
    [ "$_cactor_had_nounset" = "1" ] && set -u
    unset _cactor_had_nounset _cactor_d _cactor_envsetup
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
    # 幂等追加：envsetup.sh 也会设这两个变量，直接前置会把同一条路径加两遍。
    if [ -n "$_cactor_cj_home" ]; then
        _cactor_dyld="${_cactor_cj_home}/runtime/lib/darwin_${_cactor_arch}_cjnative:${_cactor_cj_home}/tools/lib"
        case ":${DYLD_FALLBACK_LIBRARY_PATH:-}:" in
            *:"$_cactor_dyld":*) ;;
            *) export DYLD_FALLBACK_LIBRARY_PATH="${_cactor_dyld}${DYLD_FALLBACK_LIBRARY_PATH:+:${DYLD_FALLBACK_LIBRARY_PATH}}" ;;
        esac
        case ":${DYLD_LIBRARY_PATH:-}:" in
            *:"$_cactor_dyld":*) ;;
            *) export DYLD_LIBRARY_PATH="${_cactor_dyld}${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}" ;;
        esac
        unset _cactor_dyld
    fi
    # stdx.crypto.keys 通过 dlopen 找 libcrypto：找不到 OpenSSL 3 时会落到系统自带的
    # /usr/lib/libcrypto.dylib（LibreSSL 桩），后者直接拒绝加载——
    #   WARNING: ... is loading libcrypto in an unsafe way
    # 随后 SIGABRT（Abort trap: 6），栈在 stdx.crypto.keys.GeneralPrivateKey.decodeFromPem。
    # 表现是 TLS 用例“Crashed with exit code 134”，而证书本身完全正常（有效期到 2126 年）。
    # 实测：
    #   - 仅把 Homebrew openssl@3/lib 放上 DYLD_LIBRARY_PATH / DYLD_FALLBACK_LIBRARY_PATH，
    #     RemoteTlsTest 6/6 通过；不放则 6/6 crash（v2.44 定位）；
    #   - 本机装了 openssl@3/3.4/3.5/3.6/4 多个版本，按可执行文件优先取 openssl@3（3.x ABI 稳定）。
    # 两个变量都要设：unittest_bin 直接跑时 dyld 走 DYLD_FALLBACK_LIBRARY_PATH，
    # 而 `cjpm test` 起子进程时只透传 DYLD_LIBRARY_PATH（DYLD_FALLBACK 会被丢掉）。
    # 2026-11-16 实测：同一个 unittest_bin 直接跑 70/70 通过，经 cjpm test 就 7 例 SIGABRT；
    # 把 openssl@3 放进 DYLD_LIBRARY_PATH 后
    # `cjpm test modules/tests/src/unit/remote_semantics` 70/70 通过。
    # 追加而不是前置：仓颉自己的库路径仍排在最前，openssl 只作补充，不顶掉调用方显式指定的 libcrypto。
    if [ -d /opt/homebrew/opt/openssl@3/lib ]; then
        case ":${DYLD_FALLBACK_LIBRARY_PATH:-}:" in
            *:/opt/homebrew/opt/openssl@3/lib:*) ;;
            *) export DYLD_FALLBACK_LIBRARY_PATH="${DYLD_FALLBACK_LIBRARY_PATH:+${DYLD_FALLBACK_LIBRARY_PATH}:}/opt/homebrew/opt/openssl@3/lib" ;;
        esac
        case ":${DYLD_LIBRARY_PATH:-}:" in
            *:/opt/homebrew/opt/openssl@3/lib:*) ;;
            *) export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH:+${DYLD_LIBRARY_PATH}:}/opt/homebrew/opt/openssl@3/lib" ;;
        esac
    fi
    unset _cactor_arch _cactor_cj_home _cactor_d
fi
