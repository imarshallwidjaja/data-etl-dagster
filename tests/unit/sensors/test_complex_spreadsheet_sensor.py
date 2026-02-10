"""
Unit tests for complex_spreadsheet_sensor.

Tests the one-shot error handling pattern: on ValidationError or generic
exception, log, add to processed list, attempt archive, and continue.
Non-matching intent manifests should be skipped without being marked processed.
"""

import json
from unittest.mock import Mock

import pytest
from dagster import DagsterInstance, RunRequest, SkipReason, build_sensor_context
from services.dagster.etl_pipelines.sensors.complex_spreadsheet_sensor import (
    complex_spreadsheet_sensor,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_minio_resource():
    """Create a mock MinIO resource.

    Note: We intentionally do NOT spec against the real Dagster ConfigurableResource
    (MinIOResource), because Dagster's test context will treat objects with
    `get_resource_definition()` as resource definitions and replace them.
    """

    class _MinIO:
        def list_manifests(self) -> list[str]:  # pragma: no cover
            raise NotImplementedError

        def get_manifest(self, key: str) -> dict:  # pragma: no cover
            raise NotImplementedError

        def move_to_archive(self, key: str) -> None:  # pragma: no cover
            raise NotImplementedError

    resource = Mock(spec=_MinIO)
    resource.move_to_archive = Mock()
    return resource


@pytest.fixture
def sensor_context(mock_minio_resource):
    """Build a real SensorEvaluationContext with an ephemeral DagsterInstance."""
    instance = DagsterInstance.ephemeral()
    return build_sensor_context(
        instance=instance,
        resources={"minio": mock_minio_resource},
    )


def _evaluate(sensor_context):
    """Evaluate the sensor using Dagster's public API."""
    return complex_spreadsheet_sensor.evaluate_tick(sensor_context)


# =============================================================================
# Test: ValidationError → one-shot (log, mark processed, archive, continue)
# =============================================================================


def test_validation_error_marks_processed_and_archives(
    sensor_context, mock_minio_resource
):
    """ValidationError should log, mark as processed, archive, and continue."""
    manifest_key = "manifests/bad.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = {"garbage": True}

    # No RunRequests, cursor updated with the bad key
    tick = _evaluate(sensor_context)
    assert len(tick.run_requests) == 0
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)
    # Cursor updated
    cursor_data = json.loads(sensor_context.cursor)
    assert manifest_key in cursor_data["processed_keys"]


def test_validation_error_log_contains_one_shot_guidance(
    sensor_context, mock_minio_resource, capsys
):
    """ValidationError log message should include one-shot retry guidance."""
    manifest_key = "manifests/bad.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = {"garbage": True}

    _evaluate(sensor_context)
    captured = capsys.readouterr()
    assert "one-shot" in (captured.err + captured.out).lower()


def test_validation_error_archive_failure_still_marks_processed(
    sensor_context, mock_minio_resource
):
    """Even if archiving fails, the key should still be marked as processed."""
    manifest_key = "manifests/bad.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = {"garbage": True}
    mock_minio_resource.move_to_archive.side_effect = Exception("archive boom")

    _evaluate(sensor_context)

    cursor_data = json.loads(sensor_context.cursor)
    assert manifest_key in cursor_data["processed_keys"]
    # Warning is emitted via dagster logger (stderr); asserting via cursor/side effects is sufficient.


# =============================================================================
# Test: Generic exception → one-shot (log, mark processed, archive, continue)
# =============================================================================


def test_generic_exception_marks_processed_and_archives(
    sensor_context, mock_minio_resource
):
    """Generic exception should log, mark as processed, archive, and continue."""
    manifest_key = "manifests/explode.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.side_effect = RuntimeError("boom")

    tick = _evaluate(sensor_context)
    assert len(tick.run_requests) == 0
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)
    cursor_data = json.loads(sensor_context.cursor)
    assert manifest_key in cursor_data["processed_keys"]


def test_generic_exception_log_contains_one_shot_guidance(
    sensor_context, mock_minio_resource, capsys
):
    """Generic exception log message should include one-shot retry guidance."""
    manifest_key = "manifests/explode.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.side_effect = RuntimeError("boom")

    _evaluate(sensor_context)
    captured = capsys.readouterr()
    assert "one-shot" in (captured.err + captured.out).lower()


# =============================================================================
# Test: Non-matching intent → skip without marking processed
# =============================================================================


def test_non_matching_intent_skipped_without_marking_processed(
    sensor_context, mock_minio_resource, valid_tabular_manifest_dict
):
    """Manifests with non-matching intent should be skipped, not marked processed."""
    manifest_key = "manifests/tabular.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    # This is an ingest_tabular manifest, not ingest_complex_spreadsheet
    mock_minio_resource.get_manifest.return_value = valid_tabular_manifest_dict

    tick = _evaluate(sensor_context)
    # No matching intent yields no RunRequests and no cursor update
    assert len(tick.run_requests) == 0
    assert sensor_context.cursor is None
    mock_minio_resource.move_to_archive.assert_not_called()


# =============================================================================
# Test: Valid manifest → run request, mark processed, archive
# =============================================================================


def test_valid_manifest_triggers_run_and_archives(
    sensor_context,
    mock_minio_resource,
    valid_complex_spreadsheet_manifest_dict,
):
    """Valid ingest_complex_spreadsheet manifest should trigger a run, mark processed, archive."""
    manifest_key = "manifests/complex.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = (
        valid_complex_spreadsheet_manifest_dict
    )

    tick = _evaluate(sensor_context)
    assert len(tick.run_requests) == 1
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)
    cursor_data = json.loads(sensor_context.cursor)
    assert manifest_key in cursor_data["processed_keys"]


# =============================================================================
# Test: list_manifests failure → SkipReason
# =============================================================================


def test_list_manifests_failure_yields_skip(sensor_context, mock_minio_resource):
    """If list_manifests fails, sensor should yield SkipReason and return."""
    mock_minio_resource.list_manifests.side_effect = Exception("minio down")

    tick = _evaluate(sensor_context)
    assert len(tick.run_requests) == 0
    assert tick.skip_message is not None
    assert "Error listing manifests" in tick.skip_message
