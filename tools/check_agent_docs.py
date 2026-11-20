#!/usr/bin/env python3
"""Agent 接入文档与 API 快照一致性（G4）。

`docs/agent-integration.md` 面向 pic 这类 agent 运行时，文中以反引号引用的
"模块.类型"必须是已登记的公开 API（`docs/api-snapshot/*.txt`），否则文档会随代码漂移。

只检查形如 `cactor_platform.mcp.McpServer` 的完整限定名（反引号内、无空格、无括号、无泛型）；
端点、协议方法、头名（含 '/' 或 '-'）不在检查范围。

用法：python3 tools/check_agent_docs.py
"""

import os
import re
import sys

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SNAPSHOT_DIR = os.path.join(ROOT, 'docs', 'api-snapshot')
DOC = os.path.join(ROOT, 'docs', 'agent-integration.md')

SPAN = re.compile(r'`([A-Za-z_][A-Za-z0-9_.]*)`')
HEADER = re.compile(r'^(\S+) :: (.*)$')


def load_snapshot():
    """返回 {package: [签名文本]}；签名文本是 ' :: ' 之后的部分。"""
    packages = {}
    for name in os.listdir(SNAPSHOT_DIR):
        if not name.endswith('.txt'):
            continue
        with open(os.path.join(SNAPSHOT_DIR, name), encoding='utf-8') as f:
            for line in f:
                m = HEADER.match(line.strip())
                if m:
                    packages.setdefault(m.group(1), []).append(m.group(2))
    return packages


def resolve(packages, span):
    """在已知包中找最长前缀；返回 (pkg, rest) 或 None。"""
    best = None
    for pkg in packages:
        if span == pkg or span.startswith(pkg + '.'):
            if best is None or len(pkg) > len(best):
                best = pkg
    if best is None:
        return None
    rest = span[len(best):]
    if rest.startswith('.'):
        rest = rest[1:]
    return best, rest


def signature_exists(packages, pkg, rest):
    if rest == '':
        return True
    for sig in packages[pkg]:
        if sig == rest or sig.startswith(rest + '.') or sig.startswith(rest + ' '):
            return True
        # 枚举构造器形如 `EdgeConnectResult.| ...` 已被 rest + '.' 覆盖；
        # 类型名后面也可能直接跟 ':|' 或 '<'（泛型声明）
        if sig.startswith(rest + ':') or sig.startswith(rest + '<'):
            return True
    return False


def main():
    if not os.path.exists(DOC):
        print('缺少 docs/agent-integration.md')
        return 1
    packages = load_snapshot()
    if not packages:
        print('没有找到 API 快照，请先运行 tools/api_snapshot.py --update')
        return 1
    with open(DOC, encoding='utf-8') as f:
        text = f.read()

    checked = 0
    failed = []
    seen = set()
    for m in SPAN.finditer(text):
        span = m.group(1)
        if span in seen:
            continue
        seen.add(span)
        if not (span.startswith('cactor_') or span == 'cactor' or span.startswith('cactor.')):
            continue
        if '.' not in span:
            continue
        resolved = resolve(packages, span)
        if resolved is None:
            failed.append((span, '不是已登记的模块前缀'))
            continue
        pkg, rest = resolved
        checked += 1
        if not signature_exists(packages, pkg, rest):
            failed.append((span, '快照中找不到该类型'))

    if failed:
        print('Agent 文档引用了未登记的 API：')
        for span, why in failed:
            print('  - {}：{}'.format(span, why))
        print('修改文档，或确认 API 已登记再运行 tools/api_snapshot.py --update')
        return 1
    print('Agent 接入文档检查通过（{} 个公开类型引用）'.format(checked))
    return 0


if __name__ == '__main__':
    sys.exit(main())
