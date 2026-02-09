"""
Unit tests for complex_spreadsheet_sensor.

Tests the one-shot error handling pattern: on ValidationError or generic
exception, log, add to processed list, attempt archive, and continue.
Non-matching intent manifests should be skipped without being marked processed.
"""

import json
from unittest.mock import Mock

import pytest
from dagster import RunRequest, SkipReason

from services.dagster.etl_pipelines.resources import MinIOResource
from services.dagster.etl_pipelines.sensors.complex_spreadsheet_sensor import (
    complex_spreadsheet_sensor,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_minio_resource():
    """Create a mock MinIOResource."""
    resource = Mock(spec=MinIOResource)
    resource.move_to_archive = Mock()
    return resource


@pytest.fixture
def mock_sensor_context():
    """Create a mock SensorEvaluationContext."""
    context = Mock()
    context.cursor = None
    context.log = Mock()
    context.update_cursor = Mock()
    # add_dynamic_partitions is needed for valid manifest processing
    context.instance = Mock()
    context.instance.add_dynamic_partitions = Mock()
    return context


_sensor_fn = complex_spreadsheet_sensor._raw_fn


# =============================================================================
# Test: ValidationError → one-shot (log, mark processed, archive, continue)
# =============================================================================


def test_validation_error_marks_processed_and_archives(
    mock_sensor_context, mock_minio_resource
):
    """ValidationError should log, mark as processed, archive, and continue."""
    manifest_key = "manifests/bad.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = {"garbage": True}

    results = list(_sensor_fn(mock_sensor_context, mock_minio_resource))

    # No RunRequests, cursor updated with the bad key
    assert all(not isinstance(r, RunRequest) for r in results)
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)
    mock_sensor_context.update_cursor.assert_called_once()
    cursor_data = json.loads(mock_sensor_context.update_cursor.call_args[0][0])
    assert manifest_key in cursor_data["processed_keys"]


def test_validation_error_log_contains_one_shot_guidance(
    mock_sensor_context, mock_minio_resource
):
    """ValidationError log message should include one-shot retry guidance."""
    manifest_key = "manifests/bad.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = {"garbage": True}

    list(_sensor_fn(mock_sensor_context, mock_minio_resource))

    error_msg = mock_sensor_context.log.error.call_args[0][0]
    assert "one-shot" in error_msg.lower() or "sensor is one-shot" in error_msg


def test_validation_error_archive_failure_still_marks_processed(
    mock_sensor_context, mock_minio_resource
):
    """Even if archiving fails, the key should still be marked as processed."""
    manifest_key = "manifests/bad.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = {"garbage": True}
    mock_minio_resource.move_to_archive.side_effect = Exception("archive boom")

    list(_sensor_fn(mock_sensor_context, mock_minio_resource))

    mock_sensor_context.update_cursor.assert_called_once()
    cursor_data = json.loads(mock_sensor_context.update_cursor.call_args[0][0])
    assert manifest_key in cursor_data["processed_keys"]
    mock_sensor_context.log.warning.assert_called()


# =============================================================================
# Test: Generic exception → one-shot (log, mark processed, archive, continue)
# =============================================================================


def test_generic_exception_marks_processed_and_archives(
    mock_sensor_context, mock_minio_resource
):
    """Generic exception should log, mark as processed, archive, and continue."""
    manifest_key = "manifests/explode.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.side_effect = RuntimeError("boom")

    results = list(_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert all(not isinstance(r, RunRequest) for r in results)
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)
    mock_sensor_context.update_cursor.assert_called_once()
    cursor_data = json.loads(mock_sensor_context.update_cursor.call_args[0][0])
    assert manifest_key in cursor_data["processed_keys"]


def test_generic_exception_log_contains_one_shot_guidance(
    mock_sensor_context, mock_minio_resource
):
    """Generic exception log message should include one-shot retry guidance."""
    manifest_key = "manifests/explode.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.side_effect = RuntimeError("boom")

    list(_sensor_fn(mock_sensor_context, mock_minio_resource))

    error_msg = mock_sensor_context.log.error.call_args[0][0]
    assert "one-shot" in error_msg.lower() or "sensor is one-shot" in error_msg


# =============================================================================
# Test: Non-matching intent → skip without marking processed
# =============================================================================


def test_non_matching_intent_skipped_without_marking_processed(
    mock_sensor_context, mock_minio_resource, valid_tabular_manifest_dict
):
    """Manifests with non-matching intent should be skipped, not marked processed."""
    manifest_key = "manifests/tabular.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    # This is an ingest_tabular manifest, not ingest_complex_spreadsheet
    mock_minio_resource.get_manifest.return_value = valid_tabular_manifest_dict

    results = list(_sensor_fn(mock_sensor_context, mock_minio_resource))

    # Should yield SkipReason (no new manifests matched) but NOT update cursor
    assert all(not isinstance(r, RunRequest) for r in results)
    mock_sensor_context.update_cursor.assert_not_called()
    mock_minio_resource.move_to_archive.assert_not_called()


# =============================================================================
# Test: Valid manifest → run request, mark processed, archive
# =============================================================================


def test_valid_manifest_triggers_run_and_archives(
    mock_sensor_context,
    mock_minio_resource,
    valid_complex_spreadsheet_manifest_dict,
):
    """Valid ingest_complex_spreadsheet manifest should trigger a run, mark processed, archive."""
    manifest_key = "manifests/complex.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = (
        valid_complex_spreadsheet_manifest_dict
    )

    results = list(_sensor_fn(mock_sensor_context, mock_minio_resource))

    run_requests = [r for r in results if isinstance(r, RunRequest)]
    assert len(run_requests) == 1
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)
    mock_sensor_context.update_cursor.assert_called_once()
    cursor_data = json.loads(mock_sensor_context.update_cursor.call_args[0][0])
    assert manifest_key in cursor_data["processed_keys"]


# =============================================================================
# Test: list_manifests failure → SkipReason
# =============================================================================


def test_list_manifests_failure_yields_skip(mock_sensor_context, mock_minio_resource):
    """If list_manifests fails, sensor should yield SkipReason and return."""
    mock_minio_resource.list_manifests.side_effect = Exception("minio down")

    results = list(_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
    assert "Error listing manifests" in results[0].skip_message
