#!/usr/bin/env python3
"""Fail closed on common public-repository hygiene regressions."""

from __future__ import annotations

import hashlib
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
MAX_FILE_BYTES = 1_000_000
FORBIDDEN_PARTS = {
    ".agents",
    ".claude",
    ".rhizome-authority-node-id",
    ".runtime",
    "knowledge-vault",
    "run-artifacts",
    "runs",
    "workspace",
    "__pycache__",
}
FORBIDDEN_SUFFIXES = {
    ".db",
    ".dll",
    ".dylib",
    ".exe",
    ".log",
    ".pid",
    ".sqlite",
    ".sqlite3",
    ".so",
}
REQUIRED_FILES = {
    "LICENSE",
    "README.md",
    "SECURITY.md",
    "THIRD_PARTY_NOTICES.md",
    "agent/LICENSE",
    "agent/go.mod",
    "go.mod",
    "internal/server/assets/force-graph.min.js",
}
FORBIDDEN_TEXT = {
    "shared bootstrap password": "14" + "88",
    "legacy agent codename": "only_" + "agent",
    "legacy hyphenated agent codename": "only-" + "agent",
    "research-only field-of-use restriction": "research use " + "only",
    "commercial field-of-use restriction": "non-" + "commercial",
}
FORCE_GRAPH_SHA256 = "c1f2608b89c779070502d86591ccd78ae132af74dcd97aa2bbea829b03fd4ebb"


def candidate_files() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    candidates = {line.strip().replace("\\", "/") for line in result.stdout.splitlines() if line.strip()}
    # A tracked file deleted in the working tree is absent from the snapshot a
    # contributor is validating; required-file checks below still fail closed.
    return sorted(relative for relative in candidates if (ROOT / relative).is_file())


def main() -> int:
    failures: list[str] = []
    files = candidate_files()
    present = set(files)

    for required in sorted(REQUIRED_FILES - present):
        failures.append(f"missing required file: {required}")

    for relative in files:
        path = ROOT / pathlib.PurePosixPath(relative)
        parts = set(pathlib.PurePosixPath(relative).parts)
        forbidden = sorted(parts & FORBIDDEN_PARTS)
        if forbidden:
            failures.append(f"forbidden path component {forbidden[0]}: {relative}")
        if path.suffix.lower() in FORBIDDEN_SUFFIXES:
            failures.append(f"forbidden file type {path.suffix}: {relative}")
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            failures.append(f"listed file is missing: {relative}")
            continue
        if size > MAX_FILE_BYTES:
            failures.append(f"file exceeds {MAX_FILE_BYTES} bytes: {relative} ({size})")
        if size == 0:
            failures.append(f"empty file: {relative}")
        if b"\x00" in path.read_bytes()[:8192]:
            failures.append(f"binary/NUL content: {relative}")
            continue
        if path.suffix.lower() in {".go", ".js", ".json", ".md", ".mod", ".py", ".sql", ".txt", ".yaml", ".yml"}:
            text = path.read_text(encoding="utf-8", errors="replace").lower()
            for label, marker in FORBIDDEN_TEXT.items():
                if marker.lower() in text:
                    failures.append(f"{label}: {relative}")
    asset = ROOT / "internal/server/assets/force-graph.min.js"
    if asset.is_file():
        actual = hashlib.sha256(asset.read_bytes()).hexdigest()
        if actual != FORCE_GRAPH_SHA256:
            failures.append(f"force-graph asset hash mismatch: {actual}")

    root_license = ROOT / "LICENSE"
    agent_license = ROOT / "agent/LICENSE"
    if root_license.is_file() and agent_license.is_file() and root_license.read_bytes() != agent_license.read_bytes():
        failures.append("agent/LICENSE differs from root LICENSE")

    expected_modules = {
        ROOT / "go.mod": "module github.com/Rhizome-Project/rhizome-runtime",
        ROOT / "agent/go.mod": "module github.com/Rhizome-Project/rhizome-runtime/agent",
    }
    for path, declaration in expected_modules.items():
        if path.is_file() and path.read_text(encoding="utf-8").splitlines()[0] != declaration:
            failures.append(f"unexpected module declaration: {path.relative_to(ROOT)}")

    if failures:
        print("repository hygiene check failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print(f"repository hygiene check passed ({len(files)} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
