from pathlib import Path

from dagster import AssetKey, build_input_context, build_output_context

from services.dagster.etl_pipelines.resources.run_scoped_manifest_io_manager import (
    RunScopedManifestIOManager,
)


def _output_context(run_id: str):
    return build_output_context(
        name="result",
        step_key="raw_manifest_json",
        asset_key=AssetKey("raw_manifest_json"),
        run_id=run_id,
    )


def _input_context(run_id: str):
    upstream = _output_context(run_id)
    return build_input_context(
        name="raw_manifest_json",
        upstream_output=upstream,
        asset_key=AssetKey("raw_manifest_json"),
    )


def test_output_path_isolated_by_run_id(tmp_path: Path):
    manager = RunScopedManifestIOManager(base_dir=str(tmp_path))
    a_ctx = _output_context("run-a")
    b_ctx = _output_context("run-b")

    a_path = manager._get_path(a_ctx)
    b_path = manager._get_path(b_ctx)

    assert a_path != b_path
    assert "run-a" in str(a_path)
    assert "run-b" in str(b_path)


def test_load_input_reads_matching_run_scope(tmp_path: Path):
    manager = RunScopedManifestIOManager(base_dir=str(tmp_path))

    payload_a = {"dagster_run_id": "run-a", "run_id": "mongo-a", "manifest": {}}
    payload_b = {"dagster_run_id": "run-b", "run_id": "mongo-b", "manifest": {}}

    manager.handle_output(_output_context("run-a"), payload_a)
    manager.handle_output(_output_context("run-b"), payload_b)

    loaded_a = manager.load_input(_input_context("run-a"))
    loaded_b = manager.load_input(_input_context("run-b"))

    assert loaded_a["dagster_run_id"] == "run-a"
    assert loaded_b["dagster_run_id"] == "run-b"
