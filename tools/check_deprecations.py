#!/usr/bin/env python3
"""E51：@Deprecated 注解完整性校验。

扫描 modules/*/src 下全部 @Deprecated 注解（排除测试文件），要求：
  1. 带 since 命名参数；
  2. 注解消息或紧邻注释中注明计划移除版本（Akka 惯例：since 的下一个 semver 大版本，当前即 2.0.0）。

任一缺失即列出并退出码 1。仓颉 @Deprecated 仅支持消息与 since，无 remove 参数，
因此移除版本统一写在注解消息文本中（形如「2.0.0 移除」「2.0 删除」）。
"""
import os
import re
import sys

DEPR = re.compile(r'@Deprecated\s*\[')
SINCE = re.compile(r'since\s*:')
# 消息文本或注释中的移除版本提示：2.0.0 / 2.0 + 移除|删除
REMOVE = re.compile(r'2\.0(\.0)?\s*(移除|删除)')

LIBS = ['foundation', 'core', 'runtime', 'patterns', 'remote', 'persistence',
        'cluster', 'streams', 'xstate', 'testkit', 'cactor']


def main():
    bad = []
    for m in LIBS:
        root = os.path.join('modules', m, 'src')
        if not os.path.isdir(root):
            continue
        for dp, _, fs in os.walk(root):
            for f in fs:
                if not f.endswith('.cj'):
                    continue
                path = os.path.join(dp, f)
                lines = open(path, encoding='utf-8').readlines()
                for n, line in enumerate(lines, 1):
                    if not DEPR.search(line):
                        continue
                    # 注解通常单行；若 since/移除版本写在续行，向后看 3 行
                    window = ''.join(lines[n - 1:n + 2])
                    # 连同上一行 doc/普通注释一起看（移除版本可能写在注释里）
                    prev = lines[n - 2] if n >= 2 else ''
                    ctx = window + prev
                    if not SINCE.search(ctx):
                        bad.append(f'{path}:{n}: 缺 since')
                    elif not REMOVE.search(ctx):
                        bad.append(f'{path}:{n}: 缺移除版本（消息或注释需注明，如「2.0.0 移除」）')
    if bad:
        print('以下 @Deprecated 不完整：')
        print('\n'.join(bad))
        sys.exit(1)
    print('通过')


if __name__ == '__main__':
    main()
