"""Re-wrap over-long prose paragraphs in docstrings and comment blocks.

Written because hand-wrapping long explanatory prose to a column limit is tedious and easy to
get wrong in a way that reads badly: a single word stranded on its own line looks like a typo.
This rewraps whole paragraphs rather than splitting individual lines, so the result reads the
way it would have if it had been written to the limit in the first place.

Two rules keep it from being dangerous, and the first version of this file had neither, which is
why it broke three source files on its first run:

  * **A line containing a quote is never treated as prose.** An implicitly concatenated string
    literal spread over several lines looks exactly like a wrapped paragraph, and wrapping it
    again moves the quote characters and produces a syntax error.
  * **A file that stops parsing is restored.** After rewriting, the result is fed to
    ``ast.parse``; if it fails, the original is written back and the file is reported. A
    formatter that can emit a file the interpreter cannot read is worse than no formatter,
    because its damage lands in the middle of unrelated work.

    python tools/rewrap.py --width 94 src/skewdoc/*.py
"""

from __future__ import annotations

import argparse
import ast
import re
import textwrap
from pathlib import Path

# Anything that suggests the line is code, part of a string literal, or a structured list rather
# than a paragraph of prose. Quotes are in here because of the bug described above.
CODE_HINTS = ("=", "(", ")", "[", "]", "{", "}", "|", ">>>", "::", '"', "'", "`")
BULLET = re.compile(r"^\s*([*\-+]|\d+\.)\s")


def _is_prose(line: str) -> bool:
    stripped = line.strip()
    if not stripped or BULLET.match(line):
        return False
    if stripped.startswith(('"""', "'''", "@", "#!")):
        return False
    body = stripped[2:] if stripped.startswith("# ") else stripped
    return not any(hint in body for hint in CODE_HINTS)


def rewrap(text: str, width: int) -> str:
    lines = text.splitlines()
    out: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not _is_prose(line):
            out.append(line)
            index += 1
            continue
        indent = line[: len(line) - len(line.lstrip())]
        comment = line.strip().startswith("# ")
        prefix = indent + ("# " if comment else "")
        block: list[str] = []
        start = index
        while index < len(lines) and _is_prose(lines[index]):
            candidate = lines[index]
            candidate_indent = candidate[: len(candidate) - len(candidate.lstrip())]
            candidate_comment = candidate.strip().startswith("# ")
            if candidate_indent != indent or candidate_comment != comment:
                break
            block.append(candidate.strip().removeprefix("# "))
            index += 1
        original = lines[start:index]
        if max(len(item) for item in original) <= width:
            out.extend(original)
            continue
        out.extend(
            textwrap.wrap(" ".join(block), width=width, initial_indent=prefix,
                          subsequent_indent=prefix, break_long_words=False,
                          break_on_hyphens=False)
        )
    return "\n".join(out) + ("\n" if text.endswith("\n") else "")


def process(path: Path, width: int) -> str:
    """Rewrap one file, restoring it if the result does not parse. Returns a status word."""
    original = path.read_text()
    updated = rewrap(original, width)
    if updated == original:
        return "unchanged"
    path.write_text(updated)
    if path.suffix == ".py":
        try:
            ast.parse(updated)
        except SyntaxError:
            path.write_text(original)
            return "reverted"
    return "rewrapped"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+")
    parser.add_argument("--width", type=int, default=94)
    args = parser.parse_args()
    reverted = 0
    for name in args.paths:
        path = Path(name)
        status = process(path, args.width)
        if status != "unchanged":
            print(f"{status}: {path}")
        if status == "reverted":
            reverted += 1
    if reverted:
        print(f"\n{reverted} file(s) would not parse after rewrapping and were left untouched. "
              "Wrap those paragraphs by hand.")
    return 1 if reverted else 0


if __name__ == "__main__":
    raise SystemExit(main())
