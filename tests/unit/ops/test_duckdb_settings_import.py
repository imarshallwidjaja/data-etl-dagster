"""Unit tests for DuckDB settings import behavior.

These tests ensure DuckDB settings can be imported when etl_pipelines is
loaded from the user-code container layout (no top-level `services` package).
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType


def test_duckdb_settings_import_without_services_package(monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    dagster_root = repo_root / "services" / "dagster"

    assert dagster_root.exists(), "Expected services/dagster package root"

    new_sys_path: list[str] = [str(dagster_root)]
    for entry in sys.path:
        if not entry:
            continue
        try:
            entry_path = Path(entry).resolve()
        except OSError:
            continue
        if entry_path == repo_root or repo_root in entry_path.parents:
            continue
        new_sys_path.append(entry)

    monkeypatch.setattr(sys, "path", new_sys_path)

    removed_modules: dict[str, ModuleType] = {}
    for key in list(sys.modules):
        if key == "etl_pipelines" or key.startswith("etl_pipelines."):
            removed_modules[key] = sys.modules.pop(key)
        if key == "services" or key.startswith("services."):
            removed_modules[key] = sys.modules.pop(key)

    importlib.invalidate_caches()

    try:
        module = importlib.import_module("etl_pipelines.ops.duckdb_settings")
        assert hasattr(module, "build_duckdb_join_settings")
    finally:
        for key in list(sys.modules):
            if key == "etl_pipelines" or key.startswith("etl_pipelines."):
                sys.modules.pop(key)
            if key == "services" or key.startswith("services."):
                sys.modules.pop(key)
        sys.modules.update(removed_modules)
