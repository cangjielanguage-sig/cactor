#!/usr/bin/env bash
# CActor DSL 行数对照（A1 验收：DSL 写法相对经典写法行数降幅 ≥40%）
#
# 对照对象：modules/examples/src/dsl_showcase 下的 classic/ 与 dsl/ 两个子包，
# 三个场景各一对文件（hello / counter / ask）：
#   - 文件内容只有"消息定义 + Actor 定义"，驱动代码在各自的 drivers.cj 里且完全对应，
#     这样行数差异只反映 DSL 本身带来的样板削减；
#   - 主指标 = 代码行（去掉文件头注释、行注释、空行与 import 行），对应 A1 的"代码行数"；
#   - 参考值 = 整个文件行数（含注释与 import），会被固定样板摊薄。
#
# 用法：tools/dsl_line_count.sh
set -uo pipefail

cd "$(dirname "$0")/.."

BASE=modules/examples/src/dsl_showcase

code_lines() {
    # 去掉块注释、行注释、空行与 import 行后的有效代码行数
    python3 - "$1" <<'PY'
import io, sys
path = sys.argv[1]
count = 0
in_block = False
for line in io.open(path, encoding='utf-8').read().split('\n'):
    s = line.strip()
    if in_block:
        if '*/' in s:
            in_block = False
        continue
    if s.startswith('/*'):
        if '*/' not in s:
            in_block = True
        continue
    if s.startswith('//') or s == '' or s.startswith('import '):
        continue
    count += 1
print(count)
PY
}

totalCodeClassic=0
totalCodeDsl=0
totalFileClassic=0
totalFileDsl=0

printf '%-10s %10s %8s %8s %10s %8s\n' "场景" "经典(代码)" "DSL(代码)" "降幅" "经典(整文件)" "DSL(整文件)"
for scene in hello counter ask; do
    cc=$(code_lines "${BASE}/classic/${scene}.cj")
    dc=$(code_lines "${BASE}/dsl/${scene}.cj")
    cf=$(wc -l <"${BASE}/classic/${scene}.cj")
    df=$(wc -l <"${BASE}/dsl/${scene}.cj")
    pct=$(( (cc - dc) * 100 / cc ))
    printf '%-10s %10s %8s %7s%% %10s %8s\n' "${scene}" "${cc}" "${dc}" "${pct}" "${cf}" "${df}"
    totalCodeClassic=$((totalCodeClassic + cc))
    totalCodeDsl=$((totalCodeDsl + dc))
    totalFileClassic=$((totalFileClassic + cf))
    totalFileDsl=$((totalFileDsl + df))
done

pct=$(( (totalCodeClassic - totalCodeDsl) * 100 / totalCodeClassic ))
filePct=$(( (totalFileClassic - totalFileDsl) * 100 / totalFileClassic ))
echo "---"
printf '%-10s %10s %8s %7s%% %10s %8s\n' "合计" "${totalCodeClassic}" "${totalCodeDsl}" "${pct}" "${totalFileClassic}" "${totalFileDsl}"

if [[ ${pct} -ge 40 ]]; then
    echo "A1 达标：代码行降幅 ${pct}% ≥ 40%（整文件行数降幅 ${filePct}%，含注释与 import 的固定样板）"
    exit 0
fi
echo "A1 未达标：代码行降幅 ${pct}% < 40%"
exit 1
