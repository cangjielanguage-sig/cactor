#!/usr/bin/env python3
"""CActor 质量度量（docs/plan/20261106.md 的度量表 + 20261116.md 的跟踪口径）。

为什么要这个脚本：计划里"占位关键词 ≤20""真实 sleep <50""生产代码减少 ≥2.5 万行"这些
验收条件此前只能靠人工 grep，结论不可复现，也无法判断某一批删除到底推进了多少。
这里把四个口径固定成一条命令，任何人跑出来的数字必须一致。

用法：
    python3 tools/quality_metrics.py            # 打印度量表（Markdown）
    python3 tools/quality_metrics.py --json     # 机器可读
    python3 tools/quality_metrics.py --dead     # 额外列出零引用包（E20′/E46/E47 的待删清单）
    python3 tools/quality_metrics.py --check    # 按计划的最终目标判定（未达标即退出码 1）
    python3 tools/quality_metrics.py --check --allow-placeholders xstate
                                                # 冻结模块的占位词不计入总数（D2：xstate 冻结后待 E51 移除）

--check 用的是 20261106 计划的最终目标，当前必然失败；它的用途是"把目标接进 nightly 时
不需要再改脚本"，以及本地判断某个批次有没有实质推进。日常门禁（ci.sh）只跑报表模式。

口径说明（与计划一致，不要自行放宽）：
  * 生产代码 = modules/<12 个库模块>/src 下的 .cj，**不含** tests / examples / macros；
  * 占位关键词 = 简化实现|占位|TODO|模拟实现|未实现|placeholder，统计出现次数；
  * 真实 sleep = modules/tests/src 下 `sleep(` 的调用次数（不含注释里的提及）；
  * 零引用包 = 除自身外没有任何文件（lib/test/demo 都算）import 它，且排除根重导出包。
"""

import argparse
import collections
import json
import os
import re
import sys

LIB_MODULES = [
    "foundation", "core", "runtime", "patterns", "remote",
    "persistence", "cluster", "streams", "xstate", "cactor", "testkit", "macros",
]
PLACEHOLDER = re.compile(r"简化实现|占位|TODO|模拟实现|未实现|placeholder")
SLEEP = re.compile(r"\bsleep\(")
# 包声明有多种前缀形式：package / public package / protected package / macro package
# （仓颉的宏包写作 `macro package cactor_macros.actormacro`，漏掉它会把宏包误判成死代码）。
PKG_DECL = re.compile(r"\s*(?:(?:public|protected|internal|private|macro|open|sealed)\s+)*package\s+([\w.]+)")
IMPORT = re.compile(r"\s*(?:public\s+)?import\s+([\w.]+)")
DEPRECATED = re.compile(r"@Deprecated")

# 目标的最终值（计划里的验收线）；--check 用它
TARGETS = {
    "lib_lines": 76_000 - 25_000,   # 6183072 时约 7.6 万行，M3 要求减少 ≥2.5 万行
    "placeholders": 20,
    "test_sleeps": 50,
    "dead_packages": 0,   # 死包：零消费 + 只被死包消费的子树
}


def iter_cj(root):
    for dirpath, _, files in os.walk(root):
        for name in sorted(files):
            if name.endswith(".cj"):
                yield os.path.join(dirpath, name)


def read(path):
    with open(path, encoding="utf-8", errors="replace") as fh:
        return fh.read()


def collect():
    data = {"modules": {}, "placeholders_by_module": {}, "placeholders": 0,
            "test_sleeps": 0, "test_sleep_files": 0, "deprecated": 0,
            "lib_lines": 0, "dead_packages": []}

    for module in LIB_MODULES:
        src = os.path.join("modules", module, "src")
        if not os.path.isdir(src):
            continue
        lines = 0
        hits = 0
        for path in iter_cj(src):
            text = read(path)
            lines += text.count("\n") + (0 if text.endswith("\n") else 1)
            hits += len(PLACEHOLDER.findall(text))
            data["deprecated"] += len(DEPRECATED.findall(text))
        data["modules"][module] = lines
        data["placeholders_by_module"][module] = hits
        data["placeholders"] += hits
        data["lib_lines"] += lines

    tests = os.path.join("modules", "tests", "src")
    per_file = {}
    if os.path.isdir(tests):
        for path in iter_cj(tests):
            n = len(SLEEP.findall(read(path)))
            if n:
                per_file[path] = n
                data["test_sleeps"] += n
        data["test_sleep_files"] = len(per_file)
    data["test_sleep_top"] = sorted(per_file.items(), key=lambda kv: -kv[1])[:8]

    data["dead_packages"] = dead_packages()
    return data


def _packages():
    """包名 -> 文件列表；同时返回 文件 -> 包名。"""
    pkg_files = collections.defaultdict(list)
    file_pkg = {}
    for module in LIB_MODULES + ["tests", "examples"]:
        for path in iter_cj(os.path.join("modules", module, "src")):
            pkg = None
            for line in read(path).splitlines():
                m = PKG_DECL.match(line)
                if m:
                    pkg = m.group(1)
                    break
            if pkg:
                pkg_files[pkg].append(path)
                file_pkg[path] = pkg
    return pkg_files, file_pkg


def dead_packages():
    """死包：不仅"零消费"，还包括"只被死包消费"的整片子树。

    只认 import 语句（注释行跳过）。不按符号名判断：仓库里同名方法/类型很多
    （size、toString、LogLevel、PriorityQueueMailbox…），按符号匹配会大量误报，
    这一点在 2026-11-16 的复核里踩过。

    判定用不动点：
      1. 零消费包（没有任何外部 import）先判死；
      2. 若一个包的**全部**消费者都已判死，它也判死；
      3. 重复直到不再变化。
    第 2 步能挖出"只被死代码引用"的整棵子树——2026-11-16 实测 runtime 的
    `dispatcher.batch_processing` / `runtime.message`（batch_message_processor）
    正是这种情况。

    **已知盲区（必须知道）**：本分析是**包级**的。同一包里的死文件看不出来——
    `runtime/src/dispatcher/factory/enhanced_dispatcher_factory.cj` 所在的
    `cactor_runtime.dispatcher.factory` 包是活的（`cactor_runtime.cj` 用了
    `DefaultAdvancedDispatcherFactory`），于是该包整体被判活；但
    `EnhancedDispatcherFactory` 这个类本身零引用，它连带独占的
    `dispatcher.adaptive`(437) / `dispatcher.monitoring`(731) / `dispatcher.dynamic`(302)
    实际也是死的（合计 1,803 行）。包级结果之外，还要做一遍**文件级**的类引用复核。
    """
    pkg_files, file_pkg = _packages()
    consumers = collections.defaultdict(set)
    for path, pkg in file_pkg.items():
        for line in read(path).splitlines():
            stripped = line.strip()
            if stripped.startswith("//"):
                continue
            m = IMPORT.match(line)
            if not m:
                continue
            target = m.group(1)
            while target and target not in pkg_files:
                if "." not in target:
                    target = ""
                    break
                target = target.rsplit(".", 1)[0]
            if target and target != pkg:
                consumers[target].add(path)

    roots = {"cactor", "cactor_foundation", "cactor_core", "cactor_runtime",
             "cactor_patterns", "cactor_cluster", "cactor_streams", "cactor_xstate"}
    candidates = {p for p in pkg_files
                  if not p.startswith(("cactor_tests", "cactor_examples")) and p not in roots}

    dead = {p for p in candidates if not consumers.get(p)}
    changed = True
    while changed:
        changed = False
        for pkg in candidates - dead:
            users = consumers.get(pkg, set())
            owner_pkgs = {file_pkg[f] for f in users if f in file_pkg}
            # 消费者全在死集合里 -> 这个包也只服务于死代码
            if owner_pkgs and owner_pkgs <= dead:
                dead.add(pkg)
                changed = True

    out = []
    for pkg in dead:
        files = pkg_files[pkg]
        lines = sum(read(f).count("\n") + 1 for f in files)
        out.append({"package": pkg, "lines": lines, "files": len(files)})
    out.sort(key=lambda d: -d["lines"])
    return out


def report(data):
    print("## 质量度量（python3 tools/quality_metrics.py）\n")
    print("| 指标 | 当前 | 目标 | 状态 |")
    print("|---|---|---|---|")
    rows = [
        ("库模块生产代码（行）", data["lib_lines"], TARGETS["lib_lines"]),
        ("占位关键词（次）", data["placeholders"], TARGETS["placeholders"]),
        ("测试真实 sleep（次）", data["test_sleeps"], TARGETS["test_sleeps"]),
        ("死包（个）", len(data["dead_packages"]), TARGETS["dead_packages"]),
    ]
    for name, cur, target in rows:
        print(f"| {name} | {cur} | {target} | {'✅' if cur <= target else '❌'} |")
    if "placeholders_allowed" in data:
        parts = "、".join(f"{m} {n}" for m, n in data["placeholders_allowed"].items())
        print(f"\n> 占位关键词豁免（冻结模块，不计入目标）：{parts}；有效总数 "
              f"{data['placeholders_effective']} / {TARGETS['placeholders']}")
    print(f"| @Deprecated（处） | {data['deprecated']} | 每处带 since/移除版本 | — |")
    print(f"| 含 sleep 的测试文件（个） | {data['test_sleep_files']} | — | — |")
    print("\n按模块：\n")
    print("| 模块 | 行数 | 占位关键词 |")
    print("|---|---|---|")
    for module in LIB_MODULES:
        if module in data["modules"]:
            print(f"| {module} | {data['modules'][module]} | "
                  f"{data['placeholders_by_module'][module]} |")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--dead", action="store_true", help="列出零引用包")
    parser.add_argument("--check", action="store_true", help="按计划最终目标判定")
    parser.add_argument(
        "--allow-placeholders", default="",
        help="从占位关键词总数与 --check 中豁免的模块（逗号分隔）；用于已冻结的 experimental 模块"
             "（如 xstate：D2 冻结后按 E51 到期移除，占位词不再计入总目标）")
    args = parser.parse_args()

    if not os.path.isdir("modules"):
        print("请在 workspace 根目录运行", file=sys.stderr)
        return 2

    data = collect()
    # 豁免模块的占位词不计入总数与 --check（冻结模块的占位是"待 E51 删除"的已知存量，
    # 不应污染其余模块的达标判定；按模块表仍如实列出，避免掩盖问题）
    allow = {m.strip() for m in args.allow_placeholders.split(",") if m.strip()}
    if allow:
        data["placeholders_allowed"] = {
            m: data["placeholders_by_module"].get(m, 0)
            for m in sorted(allow) if m in data["placeholders_by_module"]
        }
        data["placeholders_effective"] = data["placeholders"] - sum(data["placeholders_allowed"].values())
    data["test_sleep_top"] = [(p, n) for p, n in data.get("test_sleep_top", [])]

    if args.json:
        print(json.dumps(data, ensure_ascii=False, indent=2))
    else:
        report(data)
        if args.dead:
            print("\n零引用包（待删清单，按行数降序）：\n")
            print("| 包 | 行数 | 文件 |")
            print("|---|---|---|")
            for item in data["dead_packages"]:
                print(f"| {item['package']} | {item['lines']} | {item['files']} |")

    if args.check:
        # 有豁免时以 effective 计；无豁免时与旧行为一致（raw 总数）
        placeholders = data.get("placeholders_effective", data["placeholders"])
        failed = []
        if data["lib_lines"] > TARGETS["lib_lines"]:
            failed.append(f"生产代码 {data['lib_lines']} > {TARGETS['lib_lines']}")
        if placeholders > TARGETS["placeholders"]:
            failed.append(f"占位关键词 {placeholders} > {TARGETS['placeholders']}")
        if data["test_sleeps"] > TARGETS["test_sleeps"]:
            failed.append(f"测试 sleep {data['test_sleeps']} > {TARGETS['test_sleeps']}")
        if len(data["dead_packages"]) > TARGETS["dead_packages"]:
            failed.append(f"死包 {len(data['dead_packages'])} > 0")
        if failed:
            print("未达标：\n  " + "\n  ".join(failed), file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
