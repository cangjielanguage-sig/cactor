#!/usr/bin/env bash
# CActor PR 门禁入口（E50）：平台无关的合并请求检查入口。
#
# 本脚本只做三件事：准备仓颉环境（tools/env.sh）→ 带超时执行完整 CI（tools/ci.sh）
# → 原样透传退出码。仓库内不放置任何平台专用的流水线配置文件（如 .github / .atomgit），
# 托管平台侧仍需人工把 PR 流水线命令指向本脚本，并配置触发、分支保护与失败通知。
#
# 用法：
#   tools/ci_pr.sh              # 跑 PR 门禁（build + 分层检查 + 全部测试 + 示例自检）
#   tools/ci_pr.sh --dry-run    # 只打印将执行的命令，不真正运行（接线自检用）
#   tools/ci_pr.sh --help       # 打印本说明
#
# 环境变量：
#   CI_PR_TIMEOUT   整个门禁的超时秒数（默认 2700 = 45 min；挂死按失败处理，退出码 124）。
#
# 退出码：tools/ci.sh 的退出码原样透传；超时由 timeout 返回 124；参数错误返回 2。
set -uo pipefail

cd "$(dirname "$0")/.."

DRY_RUN=0
while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --help|-h)
            sed -n '2,22p' "$0" | grep -E '^#' | sed 's/^# \{0,1\}//'
            exit 0 ;;
        *) echo "未知参数: $1（PR 门禁不接受 --soak / --bench）" >&2; exit 2 ;;
    esac
done

# 默认 45 min：完整门禁实测约 5–10 min，留足慢机器与 flake 重试余量。
CI_PR_TIMEOUT="${CI_PR_TIMEOUT:-2700}"
if ! [[ "$CI_PR_TIMEOUT" =~ ^[0-9]+$ ]] || [ "$CI_PR_TIMEOUT" -le 0 ]; then
    echo "CI_PR_TIMEOUT 必须是正整数秒（当前: $CI_PR_TIMEOUT）" >&2
    exit 2
fi

# 与 tools/ci.sh / tools/ci_nightly.sh 一致：显式用当前 PATH 的 bash 执行，
# 避免 macOS 上 /usr/bin/env shebang 清掉 DYLD_* 导致子进程找不到仓颉运行时库。
RUN_CMD=(timeout --foreground "$CI_PR_TIMEOUT" bash tools/ci.sh)

if [ "$DRY_RUN" = "1" ]; then
    echo "  (dry-run) source tools/env.sh"
    echo "  (dry-run) ${RUN_CMD[*]}"
    echo "  (dry-run) 退出码将原样透传；超时 ${CI_PR_TIMEOUT}s 返回 124"
    exit 0
fi

printf '\n==> [%s] PR 门禁：source tools/env.sh && %s\n' "$(date +%H:%M:%S)" "${RUN_CMD[*]}"
# shellcheck disable=SC1091
source tools/env.sh
"${RUN_CMD[@]}"
rc=$?
if [ "$rc" -eq 124 ]; then
    echo "PR 门禁超时（>${CI_PR_TIMEOUT}s），按失败处理" >&2
elif [ "$rc" -ne 0 ]; then
    echo "PR 门禁失败（tools/ci.sh 退出码 ${rc}）" >&2
else
    echo "PR 门禁通过"
fi
exit "$rc"
