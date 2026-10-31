#!/usr/bin/env bash
# CActor 宏负例编译测试（对照 docs/plan/dsl20261030.md §8.4 / 验收 A2）
#
# 目的：把"编译期应该报错"的 DSL 错误固化成可重复执行的检查，
# 而不是只靠人工验证一次。
#
# 用法：tools/negative_tests.sh
# 约定：tools/negative/<case>/ 是独立探测工程（不进 workspace，避免污染正常构建）；
#       目录名对应 tools/negative/cases.txt 中的一行：<目录> <期望片段> <期望结果>
#       期望结果为 pass 表示"必须编译通过"，fail 表示"必须编译失败且错误信息包含片段"，
#       warn 表示"必须编译通过但要有指定告警"（如 deprecated）。
set -uo pipefail

cd "$(dirname "$0")/.."

if ! command -v cjpm >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    source "${CANGJIE_HOME:-$HOME/.cangjie/1.2.0/cangjie}/envsetup.sh" >/dev/null
fi

CASES=tools/negative/cases.txt
failed=0
total=0

while read -r dir expect result; do
    [[ -z "${dir:-}" || "${dir:0:1}" == "#" ]] && continue
    total=$((total + 1))
    log=$(mktemp)
    (cd "tools/negative/${dir}" && cjpm build) >"$log" 2>&1
    code=$?
    clean=$(sed 's/\x1b\[[0-9;]*m//g' "$log")
    if [[ "${result}" == "pass" ]]; then
        if [[ $code -eq 0 ]]; then
            printf '  [ OK ] %s（编译通过）\n' "${dir}"
        else
            printf '  [FAIL] %s（期望编译通过，实际失败）\n' "${dir}"
            echo "$clean" | grep -E "error|Error" | head -5
            failed=$((failed + 1))
        fi
    elif [[ "${result}" == "warn" ]]; then
        if [[ $code -eq 0 ]] && grep -qi "${expect}" <<<"$clean"; then
            printf '  [ OK ] %s（编译通过且给出「%s」告警）\n' "${dir}" "${expect}"
        else
            printf '  [FAIL] %s（期望编译通过且给出「%s」告警）\n' "${dir}" "${expect}"
            echo "$clean" | grep -iE "warning|error" | head -5
            failed=$((failed + 1))
        fi
    else
        clean=$(sed 's/\x1b\[[0-9;]*m//g' "$log")
        if [[ $code -ne 0 ]] && grep -qF "${expect}" <<<"$clean"; then
            printf '  [ OK ] %s（编译期报错且信息可读）\n' "${dir}"
        else
            printf '  [FAIL] %s（期望编译失败且报错包含「%s」）\n' "${dir}" "${expect}"
            echo "$clean" | grep -E "error|Error" | head -5
            failed=$((failed + 1))
        fi
    fi
    rm -f "$log"
done <"$CASES"

echo
if [[ $failed -eq 0 ]]; then
    echo "负例编译测试全部通过：${total}/${total}"
    exit 0
fi
echo "负例编译测试失败：$((total - failed))/${total} 通过，${failed} 失败"
exit 1
