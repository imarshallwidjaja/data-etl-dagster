"""Tests that documentation files follow the uv-first, compose.yaml conventions.

Validates:
- No stale conda/pip/docker-compose references in docs, services, tests, scripts
- Root AGENTS.md contains worktree environment guidance
- docs/agents/testing.md uses uv-first commands
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# Directories to scan (matching the verification command scope)
SCAN_DIRS = ["docs", "services", "tests", "scripts"]
SCAN_FILES = ["AGENTS.md"]

# File extensions to scan
DOC_EXTENSIONS = {".md", ".py"}

# Test files that legitimately reference banned patterns in assertions/comments
# are excluded from the banned-pattern scan to avoid self-referential failures.
_SELF = Path(__file__).resolve()
_EXCLUDED_FILES = {
    _SELF,
    _SELF.parent / "test_ci_workflow_standards.py",
}


def _collect_scannable_files() -> list[Path]:
    """Collect all files in SCAN_DIRS + SCAN_FILES that match doc extensions."""
    files: list[Path] = []
    for d in SCAN_DIRS:
        dirpath = REPO_ROOT / d
        if dirpath.is_dir():
            for f in dirpath.rglob("*"):
                if (
                    f.is_file()
                    and f.suffix in DOC_EXTENSIONS
                    and f not in _EXCLUDED_FILES
                ):
                    files.append(f)
    for f in SCAN_FILES:
        fp = REPO_ROOT / f
        if fp.is_file():
            files.append(fp)
    return sorted(files)


# ── Banned pattern tests ────────────────────────────────────────────

BANNED_PATTERNS = [
    ("conda activate", re.compile(r"conda activate")),
    ("requirements-test.txt", re.compile(r"requirements-test\.txt")),
    ("docker-compose.yaml", re.compile(r"docker-compose\.yaml")),
]


@pytest.mark.parametrize(
    "label,pattern",
    [(label, pat) for label, pat in BANNED_PATTERNS],
    ids=[label for label, _ in BANNED_PATTERNS],
)
def test_no_banned_patterns_in_docs(label: str, pattern: re.Pattern) -> None:
    """No scannable file should contain banned legacy patterns."""
    violations: list[str] = []
    for fp in _collect_scannable_files():
        try:
            text = fp.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            if pattern.search(line):
                rel = fp.relative_to(REPO_ROOT)
                violations.append(f"  {rel}:{i}: {line.strip()}")

    assert not violations, f"Found '{label}' in documentation/scripts:\n" + "\n".join(
        violations
    )


# ── Positive content tests ──────────────────────────────────────────


def test_root_agents_has_worktree_section() -> None:
    """Root AGENTS.md should document worktree environment setup."""
    text = (REPO_ROOT / "AGENTS.md").read_text()
    assert "uv" in text.lower(), "AGENTS.md should mention uv"
    assert "worktree" in text.lower(), "AGENTS.md should mention worktrees"


def test_testing_md_uses_uv_commands() -> None:
    """docs/agents/testing.md should use uv-first commands."""
    text = (REPO_ROOT / "docs" / "agents" / "testing.md").read_text()
    assert "uv run pytest" in text, "testing.md should reference 'uv run pytest'"
    assert "uv sync" in text, "testing.md should reference 'uv sync'"


def test_testing_md_has_compose_yaml() -> None:
    """docs/agents/testing.md should reference compose.yaml (not docker-compose.yaml)."""
    text = (REPO_ROOT / "docs" / "agents" / "testing.md").read_text()
    assert "compose.yaml" in text, "testing.md should reference compose.yaml"


def test_dagster_agents_references_compose_yaml() -> None:
    """services/dagster/AGENTS.md should reference compose.yaml."""
    text = (REPO_ROOT / "services" / "dagster" / "AGENTS.md").read_text()
    assert "compose.yaml" in text, "dagster AGENTS.md should reference compose.yaml"
