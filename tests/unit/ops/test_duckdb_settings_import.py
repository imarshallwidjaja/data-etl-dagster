"""Unit tests for DuckDB settings import behavior.

These tests ensure DuckDB settings can be imported when etl_pipelines is
loaded from the user-code container layout (no top-level `services` package).
"""

from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType


def _build_user_code_sys_path(repo_root: Path, dagster_root: Path) -> list[str]:
    """Build sys.path to mimic user-code container import layout.

    Keep third-party site-packages (including workspace-installed libs) but remove
    repo source roots so ``services`` cannot be resolved as a top-level package.
    """
    new_sys_path: list[str] = [str(dagster_root)]
    for entry in sys.path:
        if not entry:
            continue
        try:
            entry_path = Path(entry).resolve()
        except OSError:
            continue

        if entry_path == repo_root:
            continue

        if entry_path.is_relative_to(repo_root):
            rel = entry_path.relative_to(repo_root)
            if rel.parts and rel.parts[0] != ".venv":
                continue

        new_sys_path.append(entry)

    return new_sys_path


@contextmanager
def _isolated_import_state():
    removed_modules: dict[str, ModuleType] = {}
    for key in list(sys.modules):
        if key == "etl_pipelines" or key.startswith("etl_pipelines."):
            removed_modules[key] = sys.modules.pop(key)
        if key == "services" or key.startswith("services."):
            removed_modules[key] = sys.modules.pop(key)

    importlib.invalidate_caches()

    try:
        yield
    finally:
        for key in list(sys.modules):
            if key == "etl_pipelines" or key.startswith("etl_pipelines."):
                sys.modules.pop(key)
            if key == "services" or key.startswith("services."):
                sys.modules.pop(key)
        sys.modules.update(removed_modules)


def test_duckdb_settings_import_without_services_package(monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    dagster_root = repo_root / "services" / "dagster"

    assert dagster_root.exists(), "Expected services/dagster package root"

    new_sys_path = _build_user_code_sys_path(repo_root, dagster_root)

    monkeypatch.setattr(sys, "path", new_sys_path)

    with _isolated_import_state():
        module = importlib.import_module("etl_pipelines.ops.duckdb_settings")
        assert hasattr(module, "build_duckdb_join_settings")


def test_complex_spreadsheet_ops_import_without_services_package(monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    dagster_root = repo_root / "services" / "dagster"

    assert dagster_root.exists(), "Expected services/dagster package root"

    new_sys_path = _build_user_code_sys_path(repo_root, dagster_root)

    monkeypatch.setattr(sys, "path", new_sys_path)

    with _isolated_import_state():
        module = importlib.import_module("etl_pipelines.ops.complex_spreadsheet_ops")
        assert hasattr(module, "split_complex_spreadsheet_op")
