#!/usr/bin/env python3
"""Check that relative Markdown links resolve inside the repository."""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys
import urllib.parse


ROOT = pathlib.Path(__file__).resolve().parents[1]
INLINE_LINK = re.compile(r"!?\[[^\]]*\]\((?P<target><[^>]+>|[^\s)]+)")
REFERENCE_LINK = re.compile(r"^\s*\[[^\]]+\]:\s*(?P<target><[^>]+>|\S+)", re.MULTILINE)
EXTERNAL_SCHEMES = {"data", "http", "https", "mailto"}


def markdown_files() -> list[pathlib.Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "*.md"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    return [ROOT / pathlib.PurePosixPath(line) for line in result.stdout.splitlines() if line]


def local_target(raw: str) -> str | None:
    target = raw.strip().strip("<>")
    if not target or target.startswith("#"):
        return None
    parsed = urllib.parse.urlsplit(target)
    if parsed.scheme.lower() in EXTERNAL_SCHEMES or parsed.netloc:
        return None
    return urllib.parse.unquote(parsed.path)


def main() -> int:
    failures: list[str] = []
    checked = 0
    for markdown in markdown_files():
        text = markdown.read_text(encoding="utf-8")
        matches = list(INLINE_LINK.finditer(text)) + list(REFERENCE_LINK.finditer(text))
        for match in matches:
            relative = local_target(match.group("target"))
            if relative is None:
                continue
            checked += 1
            destination = (markdown.parent / relative).resolve()
            try:
                destination.relative_to(ROOT)
            except ValueError:
                failures.append(f"link escapes repository: {markdown.relative_to(ROOT)} -> {relative}")
                continue
            if not destination.exists():
                failures.append(f"missing link target: {markdown.relative_to(ROOT)} -> {relative}")

    if failures:
        print("Markdown link check failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print(f"Markdown link check passed ({checked} relative links)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
