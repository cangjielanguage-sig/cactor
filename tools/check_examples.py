#!/usr/bin/env python3
"""CActor 示例治理检查（docs/plan/20261116.md E54）。

问题：`modules/examples/src` 下有几十个示例，门禁只跑其中十几个，其余既没人跑、
也没有"为什么不跑"的记录。时间一长，示例会腐烂（编译不过、运行即崩、自检恒真），
而 CI 仍然全绿——这正是 E54 要堵的口子。

判定规则（任一不满足即退出码 1）：
  1. 每个 `modules/examples/src/*/` 目录都必须"在 CI 里被运行"或"在 NOT_RUN 白名单里"；
  2. 白名单条目必须对应真实目录，且理由非空、带分类前缀；
  3. 白名单里标 `not-built` 的目录，不得在 `modules/examples/cjpm.toml` 里声明为 executable
     （否则说明它其实能构建，只是没接进门禁）；
  4. 标 `bench` 的目录必须真的被 `tools/bench_gate.sh` 引用；
  5. `modules/examples/src/README.md` 的自动生成清单与当前目录一致（`--update-readme` 重写）。

CI 运行清单的判定方式：扫描 `tools/ci.sh` 及它 `guard`/调用的 `tools/*.sh`，
收集其中出现的 `cactor_examples.<name>` 可执行文件名。这样"是否被门禁运行"以脚本事实为准，
不需要在这里再维护一份会漂移的列表。
"""

import argparse
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "modules", "examples", "src")
TOML = os.path.join(ROOT, "modules", "examples", "cjpm.toml")
CI = os.path.join(ROOT, "tools", "ci.sh")
BENCH_GATE = os.path.join(ROOT, "tools", "bench_gate.sh")
README = os.path.join(SRC, "README.md")

BEGIN = "<!-- BEGIN:example-index（由 tools/check_examples.py --update-readme 生成，勿手改） -->"
END = "<!-- END:example-index -->"

# 不接进 ci.sh 的示例及理由。分类前缀用于规则校验：
#   multi-process — 需要多进程/端口/外部参数，由 tools/*.sh 编排运行
#   bench         — 计时类，由 tools/bench_gate.sh 在空闲机器上运行（规则要求确被引用）
#   measurement   — 测量类，需要参数或重复采样才有意义，不进每次门禁
#   not-built     — 没有在 cjpm.toml 里声明 executable，当前不产出二进制
#   known-failing — 已知缺陷导致自检失败，待单独修复（写明缺陷与去向）
NOT_RUN = {
    # 说明：cluster_node / cluster_crash_node / durable_reminder_demo / journal_crash /
    # lease_contend / ddata_stress / real_benchmark / scale_latency 通过各自的 tools/*.sh
    # 间接出现在 ci.sh 的文本里，脚本已判定为"运行中"，不需要在这里重复列出——
    # 重复列出会被规则 1 判为"既运行又在白名单"，反而报错。
    # ---- 没有声明 executable ----
    "api_demo": "not-built：cjpm.toml 未声明 executable（API 用法演示，代码在 main.cj）",
    "benchmark_demo": "not-built：cjpm.toml 未声明 executable",
    "config_system_demo": "not-built：cjpm.toml 未声明 executable",
    "enhanced_serialization_demo": "not-built：cjpm.toml 未声明 executable（enhanced 序列化未收敛）",
    "error_handling_demo": "not-built：目录内只有 pkg.cj，没有 main",
    # ---- 已知缺陷 ----
    "simple_serialization_demo": "known-failing：性能段除零（总耗时 0ms 时 totalTime 作除数），待修后接入",
    "test_framework_validation": "known-failing：TestActorRefFactory 用毫秒时间戳命名，同毫秒内两次 create 撞名（testkit 缺陷）",
    # ---- 测量类：需要参数或耗时长，单跑有意义、不适合每次门禁 ----
    "dsl_bench": "measurement：DSL 零开销对比，需重复采样才能判读，不进每次门禁",
    "typed_spawn_bench": "measurement：类型化 spawn 微基准，数字随负载抖动，单跑不判定",
    "micro_benchmark": "measurement：仓颉原语微基准，供 bench/compare.sh 使用",
    "idle_probe": "measurement：空闲调度探测（3 秒采样），用于调参而非门禁",
    "mem_profile": "measurement：内存剖析，输出为参考值",
}

EX_BIN = re.compile(r"cactor_examples\.([a-z0-9_]+)")
CI_SCRIPT = re.compile(r"tools/[a-z0-9_]+\.sh")


def read(path):
    with open(path, encoding="utf-8", errors="replace") as fh:
        return fh.read()


def example_dirs():
    if not os.path.isdir(SRC):
        sys.exit(f"找不到示例目录：{SRC}")
    return sorted(
        name for name in os.listdir(SRC)
        if os.path.isdir(os.path.join(SRC, name))
    )


def executable_set():
    """cjpm.toml 里声明为 executable 的包名（去掉 cactor_examples. 前缀）。"""
    text = read(TOML)
    names = set()
    for block in re.split(r"\[package\.package-configuration\.", text)[1:]:
        head = block.split("]", 1)[0].strip().strip('"')
        if head.startswith("cactor_examples.") and 'output-type = "executable"' in block.split("[", 1)[0]:
            names.add(head[len("cactor_examples."):])
    return names


def ci_run_set():
    """扫描 ci.sh 及其调用的 tools/*.sh，收集实际运行的示例二进制名。"""
    seen = set()
    scripts = [CI]
    text = read(CI)
    for rel in sorted(set(CI_SCRIPT.findall(text))):
        path = os.path.join(ROOT, rel)
        if os.path.isfile(path):
            scripts.append(path)
    for path in scripts:
        seen.update(EX_BIN.findall(read(path)))
    return seen


def bench_set():
    return set(EX_BIN.findall(read(BENCH_GATE)))


def check():
    problems = []
    dirs = example_dirs()
    built = executable_set()
    run = ci_run_set()
    bench = bench_set()

    unknown_whitelist = sorted(set(NOT_RUN) - set(dirs))
    for name in unknown_whitelist:
        problems.append(f"白名单条目 {name} 没有对应目录（删掉这条，或目录改名了）")

    for name in dirs:
        if name in run:
            if name in NOT_RUN:
                problems.append(f"{name} 既在 ci.sh 里运行又在白名单里，删掉白名单条目")
            continue
        reason = NOT_RUN.get(name)
        if not reason:
            problems.append(
                f"{name} 既没有在 tools/ci.sh 里运行，也不在 NOT_RUN 白名单里"
                "（要么接入门禁，要么写明不运行的理由）"
            )
            continue
        category = reason.split("：", 1)[0]
        if category not in ("multi-process", "bench", "measurement", "not-built", "known-failing"):
            problems.append(f"{name} 的白名单理由缺少分类前缀（multi-process/bench/measurement/not-built/known-failing）：{reason}")
        if category == "not-built" and name in built:
            problems.append(f"{name} 标为 not-built，但 cjpm.toml 已声明 executable；应接入门禁或改分类")
        if category == "bench" and name not in bench:
            problems.append(f"{name} 标为 bench，但 tools/bench_gate.sh 没有引用它")

    if problems:
        print("示例治理检查失败：")
        for p in problems:
            print(f"  ✗ {p}")
        return 1

    print(f"示例治理检查：通过（{len(dirs)} 个目录，CI 运行 {len([d for d in dirs if d in run])} 个，"
          f"白名单 {len(NOT_RUN)} 个）")
    return 0


def render_index():
    dirs = example_dirs()
    run = ci_run_set()
    rows = [
        "| 示例 | 是否在门禁运行 | 说明 |",
        "|---|---|---|",
    ]
    for name in dirs:
        if name in run:
            status = "✅ 运行"
            note = "自检退出码 0"
        else:
            status = "—"
            note = NOT_RUN.get(name, "（未说明）")
        rows.append(f"| `{name}` | {status} | {note} |")
    return "\n".join(rows)


def update_readme():
    text = read(README)
    block = f"{BEGIN}\n\n## 示例清单（自动生成）\n\n{render_index()}\n\n{END}"
    if BEGIN in text and END in text:
        head, rest = text.split(BEGIN, 1)
        _, tail = rest.split(END, 1)
        new = head + block + tail
    else:
        new = text.rstrip("\n") + "\n\n" + block + "\n"
    with open(README, "w", encoding="utf-8") as fh:
        fh.write(new)
    print(f"已更新 {os.path.relpath(README, ROOT)}")


def check_readme():
    text = read(README)
    if BEGIN not in text or END not in text:
        print("README 缺少自动生成清单，运行 python3 tools/check_examples.py --update-readme")
        return 1
    current = text.split(BEGIN, 1)[1].split(END, 1)[0].strip()
    expected = f"## 示例清单（自动生成）\n\n{render_index()}"
    if current != expected:
        print("README 示例清单与目录/门禁不一致，运行 python3 tools/check_examples.py --update-readme")
        return 1
    return 0


def main():
    ap = argparse.ArgumentParser(description="CActor 示例治理检查（E54）")
    ap.add_argument("--update-readme", action="store_true", help="重写 README 的示例清单")
    args = ap.parse_args()
    if args.update_readme:
        update_readme()
        return 0
    rc = check()
    rc |= check_readme()
    return rc


if __name__ == "__main__":
    sys.exit(main())
