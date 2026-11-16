#!/usr/bin/env bash
# CActor 宏公开面检查（对照 docs/plan/dsl20261030.md §9 P5.1 / 验收 A5）
#
# 目标：宏包只暴露"真实可用"的宏，不再有占位宏污染公开面。
# 当前公开宏：
#   @actor         —— 类内 onXxx(msg, ctx) → 生成类型分派 receive（支持 cases / protocol 清单）
#   @actorReceive  —— @actor 的兼容别名（保留一个版本周期）
#   @Message       —— 生成 messageType、二进制编解码器、registerCodec
#   @StateMachine  —— 状态枚举 + 转换表 → 状态机骨架与编译期检查
# 说明：原计划的 @within / @retry 因属性宏表达式歧义（§12 假设 B 证伪）改为运行时组合子
#       （cactor_core.dsl.within / retry），因此不再出现在宏包里。
set -uo pipefail

cd "$(dirname "$0")/.."

ALLOWED="actor actorReceive Message StateMachine"

# 不用 mapfile / readarray：它们是 bash 4+ 内建，而 macOS 自带的 /bin/bash 是 3.2。
# tools/ci.sh 用 `bash tools/...` 调子脚本时拿到的是 PATH 里的 bash（本机 3.2），
# 2026-11-16 实测会以 “mapfile: command not found” + “found: unbound variable”
# 把门禁判红，而宏公开面本身没问题。用可移植的逐行读取，两个版本都成立。
found=$(grep -rhoE "^public macro [A-Za-z_][A-Za-z0-9_]*" modules/macros/src --include=*.cj | awk '{print $3}' | sort -u)

if [ -z "$found" ]; then
    echo "宏公开面检查：未找到任何宏（异常）"
    exit 1
fi

bad=0
for name in $found; do
    if ! printf '%s\n' $ALLOWED | grep -qx "$name"; then
        echo "宏公开面检查：发现不在白名单里的宏 @${name}"
        bad=1
    fi
done

missing=""
for name in $ALLOWED; do
    if ! printf '%s\n' "$found" | grep -qx "$name"; then
        missing="${missing} @${name}"
    fi
done

echo "宏公开面（modules/macros）：$(printf '%s' "$found" | tr '\n' ' ')"
if [[ -n "${missing}" ]]; then
    echo "宏公开面检查：缺少期望的宏${missing}"
    bad=1
fi

if [[ $bad -eq 0 ]]; then
    echo "宏公开面检查：通过（无占位宏）"
    exit 0
fi
exit 1
