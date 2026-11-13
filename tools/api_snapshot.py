#!/usr/bin/env python3
"""CActor 公开 API 快照（E51 / DA6）。

把每个库模块的公开声明（public class / struct / enum / interface / func / prop / let / var / init /
type / macro，以及公开类型里的公开成员、接口成员、枚举构造器、带接口的 extend）抽成一行一条的签名，
写到 docs/api-snapshot/<模块>.txt。CI 用 --check 模式比对：签名有任何未登记的变化就失败，
迫使"改公共 API"成为显式动作（同时更新快照与 CHANGELOG）。

用法：
    python3 tools/api_snapshot.py            # 等同 --check：与已提交的快照比对，有差异时打印 diff 并退出 1
    python3 tools/api_snapshot.py --update   # 重新生成快照（有意修改公开 API 后执行，并在 CHANGELOG 记录）

为什么不用编译器产物：cjc 没有稳定的公开签名导出格式，.cjo 是二进制、随编译器版本变化；
这里只做轻量的词法扫描（去注释与字符串、跟踪花括号层级），确定、快速（全仓 < 1 s），不需要先构建。
签名做空白归一化并排序，代码重排、改注释、改函数体都不会引起快照变化。
"""

import os
import re
import sys
import difflib

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
SNAPSHOT_DIR = os.path.join(ROOT, 'docs', 'api-snapshot')
# 库模块（发布到中心仓的那些）；tests / examples 不是公开 API
MODULES = ['foundation', 'core', 'runtime', 'patterns', 'remote', 'persistence', 'cluster',
           'streams', 'xstate', 'testkit', 'cactor', 'macros']

MODIFIERS = r'(?:public|open|abstract|sealed|static|mut|override|redef|operator|const|unsafe|foreign)'
DECL_KW = r'(class|struct|enum|interface|func|prop|let|var|init|type|macro|main)'
PUBLIC_DECL = re.compile(r'^public\s+(?:' + MODIFIERS + r'\s+)*' + DECL_KW + r'\b')
# 接口成员没有可见性修饰也是公开的
IFACE_MEMBER = re.compile(r'^(?:' + MODIFIERS + r'\s+)*(func|prop|mut\s+prop)\b')
TYPE_DECL = re.compile(r'^(?:(?:public|private|protected|internal|open|abstract|sealed|foreign)\s+)*'
                       r'(class|struct|enum|interface)\s+([A-Za-z_][A-Za-z0-9_]*)')
EXTEND_DECL = re.compile(r'^extend\b')
ENUM_CTOR = re.compile(r'^\|\s*[A-Za-z_]')
PACKAGE = re.compile(r'^\s*(?:macro\s+)?package\s+([\w.]+)', re.M)


def strip_code(text):
    """去掉注释与字符串内容（保留引号占位），使花括号计数只反映代码结构。
    字符串插值 ${...} 里可以再嵌字符串，按递归处理。"""
    out = []
    i, n = 0, len(text)

    def skip_string(i, quote):
        # i 指向开引号之后；返回闭引号之后的位置
        triple = quote * 3
        multi = text.startswith(triple, i - 1)
        if multi:
            i += 2
        while i < n:
            c = text[i]
            if c == '\\':
                i += 2
                continue
            if c == '$' and i + 1 < n and text[i + 1] == '{':
                i = skip_interp(i + 2)
                continue
            if multi:
                if text.startswith(triple, i):
                    return i + 3
            elif c == quote:
                return i + 1
            elif c == '\n':
                return i  # 防御：单行字符串不跨行
            i += 1
        return i

    def skip_interp(i):
        depth = 1
        while i < n and depth > 0:
            c = text[i]
            if c in '"\'':
                i = skip_string(i + 1, c)
                continue
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
            i += 1
        return i

    while i < n:
        c = text[i]
        if text.startswith('//', i):
            j = text.find('\n', i)
            i = n if j < 0 else j
            continue
        if text.startswith('/*', i):
            depth, i = 1, i + 2
            while i < n and depth > 0:
                if text.startswith('/*', i):
                    depth, i = depth + 1, i + 2
                elif text.startswith('*/', i):
                    depth, i = depth - 1, i + 2
                else:
                    if text[i] == '\n':
                        out.append('\n')
                    i += 1
            continue
        if c in '"\'':
            # 字符字面量 r'x' / 'x' 与字符串一并处理
            j = skip_string(i + 1, c)
            out.append(c + c)
            out.append('\n' * text.count('\n', i, j))
            i = j
            continue
        out.append(c)
        i += 1
    return ''.join(out)


def normalize(sig):
    sig = re.sub(r'\s+', ' ', sig).strip()
    sig = re.sub(r'\s*([(),:<>\[\]])\s*', r'\1', sig)
    sig = re.sub(r',', ', ', sig)
    sig = re.sub(r':', ': ', sig)
    sig = sig.replace('<: ', ' <: ').replace(' <:', ' <:')
    return sig


def signature_at(lines, idx):
    """从 idx 行开始取完整声明头：拼接到括号配平，截掉函数体 / 初始化表达式 / 属性体。"""
    buf, depth = '', 0
    j = idx
    while j < len(lines):
        line = lines[j]
        buf += ' ' + line
        depth += line.count('(') - line.count(')') + line.count('[') - line.count(']')
        if depth <= 0:
            break
        j += 1
    sig = buf
    # 截掉体：第一个不在括号内的 '{' 或 '='（'=>' 不算，默认参数在括号内不受影响）
    d = 0
    for k, ch in enumerate(sig):
        if ch in '([':
            d += 1
        elif ch in ')]':
            d -= 1
        elif d == 0 and ch == '{':
            sig = sig[:k]
            break
        elif d == 0 and ch == '=' and sig[k + 1:k + 2] != '>' and sig[k - 1:k] not in '!<>=':
            sig = sig[:k]
            break
    return normalize(sig), j


def scan_file(path):
    with open(path, encoding='utf-8') as f:
        raw = f.read()
    m = PACKAGE.search(raw)
    pkg = m.group(1) if m else '?'
    lines = strip_code(raw).split('\n')
    entries = []
    # 容器栈：(进入时的花括号层级, 名字, 种类, 成员是否可能公开)
    stack = []
    depth = 0
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        container = stack[-1] if stack and stack[-1][0] == depth - 1 else None
        at_top = depth == 0
        record = None
        opens = None
        if line:
            tm = TYPE_DECL.match(line)
            if (at_top or (container and container[3])) and PUBLIC_DECL.match(line):
                sig, i_end = signature_at(lines, i)
                record = sig
                if tm:
                    opens = (tm.group(2), tm.group(1), True)
                i = i_end
                line = lines[i].strip() if i < len(lines) else ''
            elif at_top and tm:
                opens = (tm.group(2), tm.group(1), False)
            elif at_top and EXTEND_DECL.match(line):
                sig, i_end = signature_at(lines, i)
                if '<:' in sig:
                    record = sig  # extend 让类型实现了新接口：属于公开面
                opens = (sig[len('extend '):].split(' <:')[0], 'extend', True)
                i = i_end
                line = lines[i].strip() if i < len(lines) else ''
            elif container and container[3] and container[2] == 'interface' and IFACE_MEMBER.match(line):
                sig, i_end = signature_at(lines, i)
                record = sig
                i = i_end
                line = lines[i].strip() if i < len(lines) else ''
            elif container and container[3] and container[2] == 'enum' and ENUM_CTOR.match(line):
                record = normalize(line.split('{')[0])
        if record:
            prefix = '' if at_top else container[1] + '.'
            if not at_top and container[2] == 'extend':
                prefix = 'extend ' + container[1] + '.'
            entries.append(pkg + ' :: ' + prefix + record)
        # 花括号层级：声明行（可能多行）里的 '{' 打开容器
        full = lines[i] if i < len(lines) else ''
        o, c = full.count('{'), full.count('}')
        if opens and o > 0:
            stack.append((depth, opens[0], opens[1], opens[2]))
        depth += o - c
        while stack and stack[-1][0] >= depth:
            stack.pop()
        i += 1
    return entries


def module_snapshot(module):
    src = os.path.join(ROOT, 'modules', module, 'src')
    entries = []
    for dp, dns, fs in os.walk(src):
        dns.sort()
        for f in sorted(fs):
            if f.endswith('.cj') and not f.endswith('_test.cj'):
                entries.extend(scan_file(os.path.join(dp, f)))
    header = ('# CActor 公开 API 快照：modules/%s（tools/api_snapshot.py 生成，勿手改）\n'
              '# 有意修改公开 API 后执行 python3 tools/api_snapshot.py --update，并在 CHANGELOG 记录\n' % module)
    return header + ''.join(e + '\n' for e in sorted(set(entries)))


def main(argv):
    update = '--update' in argv
    os.makedirs(SNAPSHOT_DIR, exist_ok=True) if update else None
    changed = []
    for module in MODULES:
        if not os.path.isdir(os.path.join(ROOT, 'modules', module, 'src')):
            continue
        current = module_snapshot(module)
        path = os.path.join(SNAPSHOT_DIR, module + '.txt')
        old = open(path, encoding='utf-8').read() if os.path.exists(path) else ''
        if current == old:
            continue
        if update:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(current)
            print('已更新 %s' % os.path.relpath(path, ROOT))
        else:
            changed.append(module)
            sys.stdout.writelines(difflib.unified_diff(
                old.splitlines(True), current.splitlines(True),
                'docs/api-snapshot/%s.txt（已提交）' % module, '%s（当前源码）' % module))
    if changed:
        print('\n公开 API 与快照不一致：%s' % ', '.join(changed))
        print('若是有意修改：python3 tools/api_snapshot.py --update，提交快照并在 CHANGELOG 记录（破坏性变更须注明）')
        return 1
    if not update:
        print('公开 API 快照一致（%d 个模块）' % len(MODULES))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
