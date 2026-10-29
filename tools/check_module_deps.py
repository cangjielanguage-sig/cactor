#!/usr/bin/env python3
"""检查 CActor workspace 的模块分层约束。

cjpm 本身会拒绝循环依赖和未声明的依赖；本脚本额外保证依赖只能“向下”：
每个模块只能 import 层级比自己低的模块。

用法: python3 tools/check_module_deps.py [workspace根目录]
"""
import os
import re
import sys

# 层级越小越底层；同层模块之间不允许互相依赖
LAYERS = {
    "cactor_foundation": 0,
    "cactor_macros": 0,
    "cactor_core": 1,
    "cactor_runtime": 2,
    "cactor_remote": 2,
    "cactor_persistence": 2,
    "cactor_patterns": 3,
    "cactor": 4,
    "cactor_streams": 5,
    "cactor_testkit": 5,
    "cactor_cluster": 5,
    "cactor_distribution": 6,
    "cactor_xstate": 6,
    "cactor_integration": 6,
    "cactor_tests": 7,
    "cactor_examples": 7,
}

IMPORT_RE = re.compile(
    r"^\s*(?:(?:public|protected|internal|private)\s+)?import\s+([\w.]+)", re.M)
NAME_RE = re.compile(r'^name\s*=\s*"([\w]+)"', re.M)


def main() -> int:
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    modules_dir = os.path.join(root, "modules")
    errors = []
    for d in sorted(os.listdir(modules_dir)):
        toml = os.path.join(modules_dir, d, "cjpm.toml")
        if not os.path.isfile(toml):
            continue
        name = NAME_RE.search(open(toml, encoding="utf-8").read()).group(1)
        if name not in LAYERS:
            errors.append(f"{toml}: 模块 {name} 未登记层级")
            continue
        for dp, _, files in os.walk(os.path.join(modules_dir, d, "src")):
            for f in files:
                if not f.endswith(".cj"):
                    continue
                path = os.path.join(dp, f)
                text = open(path, encoding="utf-8", errors="ignore").read()
                for imp in IMPORT_RE.findall(text):
                    target = imp.split(".")[0]
                    if target == name or target not in LAYERS:
                        continue
                    if LAYERS[target] >= LAYERS[name]:
                        errors.append(
                            f"{path}: {name}(L{LAYERS[name]}) 不允许依赖 "
                            f"{target}(L{LAYERS[target]})")
    for e in errors:
        print(e)
    print(f"模块分层检查: {'失败' if errors else '通过'} ({len(errors)} 个问题)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
