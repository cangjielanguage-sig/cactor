#!/usr/bin/env bash
# CActor nightly 门禁（E50）：完整 CI + 长时间 soak + 压测矩阵 + 并行集群长压测。
# 默认 CI（tools/ci.sh）只跑快速步骤；nightly 每天一次（建议凌晨），覆盖：
#   1. tools/ci.sh --soak 600          完整门禁 + 600 s 稳定性 soak
#   2. tools/stress_matrix.sh quick    压测矩阵（线程扩展 / 规模扫描 / 短 soak / 背压）
#   3. tools/cluster_parallel_soak.sh 10 8   集群套件 8 进程并行 10 轮（回归 N0：并行下零 SIGSEGV）
#   4. tools/mixed_version_soak.sh     混合版本（旧线协议模拟 + kill -9 + 原端口重启）多进程验证（DA6 / DA4 / DA8）
#
# 接线建议（atomgit 平台本机无法验证，按平台文档配置）：
#   - PR 流水线：触发 tools/ci.sh（不带 --soak / --bench），必须绿色才允许合并；
#   - nightly 流水线：定时触发本脚本，失败发通知；产物（bench/results/、日志）按平台保留策略归档。
# 在 atomgit 上，上面的流水线通常配置为仓库根目录的 .atomgit/pipeline.yml（YAML，按平台的
# stages / jobs / triggers 语法）；如果平台使用其他配置文件名，以平台文档为准，接线方式不变。
#
# 用法：
#   tools/ci_nightly.sh                 # 完整 nightly
#   tools/ci_nightly.sh --dry-run       # 只打印将执行的步骤，不真正运行（参数自检用）
#   tools/ci_nightly.sh --skip-ci       # 跳过第 1 步（已单独跑过 ci.sh 时）
#   SOAK_SECS=300 tools/ci_nightly.sh   # 调整 soak 时长（默认 600）
#
# 退出码：任何一步失败即非 0；每步都有 timeout 防挂死（挂死按失败处理，不无限等待）。
set -uo pipefail

cd "$(dirname "$0")/.."

RUN_CI=1
DRY_RUN=0
while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --skip-ci) RUN_CI=0; shift ;;
        --help|-h)
            sed -n '2,40p' "$0" | grep -E '^#' | sed 's/^# \{0,1\}//'
            exit 0 ;;
        *) echo "未知参数: $1"; exit 2 ;;
    esac
done

SOAK_SECS="${SOAK_SECS:-600}"
# 各步超时（秒）：soak 本身限时 + 余量；矩阵与并行套件按经验上限。挂死 = 失败（timeout 退出码 124）。
CI_TIMEOUT=$((SOAK_SECS + 1800))       # ci.sh 全量测试约 3 min，留 soak 与基准余量
MATRIX_TIMEOUT=2700                    # stress_matrix quick 实测 < 15 min，留一倍余量
CLUSTER_TIMEOUT=2400                   # 集群套件 10 轮 × 8 进程，每轮约 15–45 s，留足余量
MIXED_TIMEOUT=600                      # mixed_version_soak 单次约 1 min，串行 3 次 + 余量

# macOS：/usr/bin/env shebang 会清掉 DYLD_*，子进程找不到仓颉运行时库（见 tools/ci.sh 的 guard）。
# 显式用当前 PATH 里的 bash 执行 shell 步骤，保留 env.sh 已准备的环境变量。
run_step() { # run_step <超时秒> <命令...>
    local t=$1; shift
    printf '\n==> [%s] %s\n' "$(date +%H:%M:%S)" "$*"
    if [ "$DRY_RUN" = "1" ]; then
        echo "  (dry-run) timeout ${t}s bash -c '$*'"
        return 0
    fi
    timeout --foreground "$t" bash -c "$*"
}

start=$(date +%s)
fail=0

if [ "$RUN_CI" = "1" ]; then
    run_step "$CI_TIMEOUT" "source tools/env.sh && CACTOR_LOG_LEVEL=off tools/ci.sh --soak ${SOAK_SECS}" \
        || { echo "nightly：完整 CI（含 soak ${SOAK_SECS}s）失败"; fail=1; }
fi

run_step "$MATRIX_TIMEOUT" "source tools/env.sh && tools/stress_matrix.sh quick" \
    || { echo "nightly：压测矩阵失败"; fail=1; }

run_step "$CLUSTER_TIMEOUT" "source tools/env.sh && tools/cluster_parallel_soak.sh 10 8" \
    || { echo "nightly：并行集群套件 10 轮失败"; fail=1; }

run_step "$MIXED_TIMEOUT" "source tools/env.sh && tools/mixed_version_soak.sh" \
    || { echo "nightly：混合版本多进程验证失败"; fail=1; }

secs=$(( $(date +%s) - start ))
if [ "$fail" = "0" ]; then
    echo "nightly 全部通过（用时 ${secs}s）"
else
    echo "nightly 有失败步骤（用时 ${secs}s），见上方输出"
fi
exit "$fail"
