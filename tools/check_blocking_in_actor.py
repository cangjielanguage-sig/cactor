#!/usr/bin/env python3
"""
A1b: 扫描 Actor 消息处理函数里的阻塞调用。

在默认（亲和池）调度器上，receive / onXxx 里的 sleep、同步文件 IO、阻塞式 foreign 调用，
会占住整个工作线程，拖慢亲和在同一个 worker 上的其他 Actor（见 docs/plan/20261117-actor.md §2.2/§4.1）。
这类调用应改用 DispatcherMode.ThreadPerActor 或 DispatcherMode.Blocking。

判定规则（启发式，宁可少报也不制造大量噪音）：
  * 只看函数名是 receive / onMessage / onXxx 的函数体（@actorReceive 宏生成的处理方法形如 onXxx）；
  * 函数体内出现 sleep(、Thread.sleep、awaitCondition(、.readln(、File./BufferedFile 的同步读写、
    std.fs 的同步调用、foreign 关键字 → 报告；
  * 函数头或函数体内任意一行含 `// blocking-ok` → 豁免（用于确实需要同步语义的地方）；
  * tools/blocking_allowlist.txt 中的路径前缀整文件豁免（测试里故意构造阻塞场景）。

退出码：发现未豁免的命中 → 1；否则 0。
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ALLOWLIST = ROOT / "tools" / "blocking_allowlist.txt"
SKIP_DIRS = {"target", ".cjpm", ".git", "node_modules"}

FUNC_RE = re.compile(r"^\s*(?:public\s+|protected\s+|private\s+)?(?:open\s+)?func\s+(receive|onMessage|on[A-Z][A-Za-z0-9_]*)\s*\(")
PATTERNS = [
    (re.compile(r"(?<![A-Za-z0-9_])sleep\s*\("), "sleep("),
    (re.compile(r"Thread\s*\.\s*sleep\s*\("), "Thread.sleep"),
    (re.compile(r"(?<![A-Za-z0-9_])awaitCondition\s*\("), "awaitCondition("),
    (re.compile(r"\.\s*readln\s*\("), "readln("),
    (re.compile(r"(?<![A-Za-z0-9_])File\s*\."), "File.* (blocking fs)"),
    (re.compile(r"(?<![A-Za-z0-9_])BufferedFile\b"), "BufferedFile (blocking fs)"),
    (re.compile(r"(?<![A-Za-z0-9_])foreign\b"), "foreign (blocking FFI)"),
]
MARKER = "// blocking-ok"


def load_allowlist() -> list[str]:
    if not ALLOWLIST.exists():
        return []
    entries = []
    for raw in ALLOWLIST.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            entries.append(line)
    return entries


def iter_sources(args: list[str]) -> list[Path]:
    roots = [Path(a) for a in args] if args else [ROOT / "modules"]
    out: list[Path] = []
    for root in roots:
        root = root if root.is_absolute() else ROOT / root
        if root.is_file() and root.suffix == ".cj":
            out.append(root)
            continue
        for p in sorted(root.rglob("*.cj")):
            if any(part in SKIP_DIRS for part in p.parts):
                continue
            out.append(p)
    return out


def strip_line(line: str) -> str:
    """去掉字符串字面量与行注释，避免把字符串里的字样当成调用。"""
    # 简单的状态机：忽略 "..." 与 '...'（仓颉里 'x' 是字符串，r'x' 是 Rune）
    result = []
    i, n = 0, len(line)
    in_str = False
    while i < n:
        ch = line[i]
        if in_str:
            if ch == "\\" and i + 1 < n:
                i += 2
                continue
            if ch == '"':
                in_str = False
            i += 1
            continue
        if ch == '"':
            in_str = True
            i += 1
            continue
        if ch == "/" and i + 1 < n and line[i + 1] == "/":
            break
        result.append(ch)
        i += 1
    return "".join(result)


def scan_file(path: Path, allow: list[str]) -> list[tuple[int, int, str, str]]:
    rel = path.relative_to(ROOT).as_posix()
    if any(rel.startswith(a) for a in allow):
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    hits = []
    i = 0
    while i < len(lines):
        m = FUNC_RE.search(strip_line(lines[i]))
        if not m:
            i += 1
            continue
        start = i + 1  # 1-based 行号
        # 找到函数体的 '{'
        j = i
        depth = 0
        opened = False
        body: list[tuple[int, str]] = []
        while j < len(lines):
            code = strip_line(lines[j])
            for ch in code:
                if ch == "{":
                    depth += 1
                    opened = True
                elif ch == "}":
                    depth -= 1
            body.append((j + 1, lines[j]))
            if opened and depth <= 0:
                break
            j += 1
        body_text = "\n".join(code for _, code in ((k, strip_line(v)) for k, v in body))
        if MARKER in body_text or MARKER in lines[i]:
            i = j + 1
            continue
        for line_no, raw in body:
            code = strip_line(raw)
            for rx, label in PATTERNS:
                if rx.search(code):
                    hits.append((start, line_no, label, raw.strip()))
                    break
        i = j + 1
    return hits


def main() -> int:
    allow = load_allowlist()
    files = iter_sources(sys.argv[1:])
    total = 0
    for path in files:
        try:
            hits = scan_file(path, allow)
        except OSError as exc:
            print(f"读取失败: {path}: {exc}", file=sys.stderr)
            continue
        for func_line, line_no, label, snippet in hits:
            total += 1
            print(
                f"{path.relative_to(ROOT)}:{line_no}: 消息处理方法（第 {func_line} 行）中出现阻塞调用 {label}\n"
                f"    {snippet}\n"
                f"    → 改用 DispatcherMode.ThreadPerActor / Blocking；确属必要时在该方法内加 // blocking-ok",
                file=sys.stderr,
            )
    if total:
        print(f"\n共 {total} 处疑似阻塞调用（A1b）。", file=sys.stderr)
        return 1
    print("A1b: 未发现消息处理方法中的阻塞调用。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
