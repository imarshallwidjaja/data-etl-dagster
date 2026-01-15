"""
Unit tests for manifest_sensor (legacy).

This sensor is intentionally limited to launching `ingest_job` only and
skips intents handled by asset sensors (tabular/join).
"""

import json
import os
import pytest
from unittest.mock import Mock, patch
from dagster import SkipReason, RunRequest
from pydantic import ValidationError

from services.dagster.etl_pipelines.sensors.manifest_sensor import manifest_sensor
from services.dagster.etl_pipelines.resources import MinIOResource


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
    return context


# Access the raw function from the sensor
_manifest_sensor_fn = manifest_sensor._raw_fn


# =============================================================================
# Test: No manifests found
# =============================================================================


def test_no_manifests_skips(mock_sensor_context, mock_minio_resource):
    """Test that sensor yields SkipReason when no manifests found."""
    mock_minio_resource.list_manifests.return_value = []

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
    assert "No new manifests" in results[0].skip_message


# =============================================================================
# Test: Valid manifest - ingest lane (default)
# =============================================================================


@patch.dict(os.environ, {}, clear=False)
def test_valid_manifest_yields_run_request_ingest_lane(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    """Test that valid manifest with default intent routes to ingest lane."""
    manifest_key = "manifests/batch_001.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    assert isinstance(results[0], RunRequest)
    run_request = results[0]

    # Check lane-prefixed run_key
    assert run_request.run_key == f"ingest:{valid_manifest_dict['batch_id']}"

    # Check job name
    assert run_request.job_name == "ingest_job"

    # Check that manifest is passed as op input to load_to_postgis
    assert "load_to_postgis" in run_request.run_config["ops"]
    assert "inputs" in run_request.run_config["ops"]["load_to_postgis"]
    assert "manifest" in run_request.run_config["ops"]["load_to_postgis"]["inputs"]
    assert (
        "value"
        in run_request.run_config["ops"]["load_to_postgis"]["inputs"]["manifest"]
    )
    assert (
        run_request.run_config["ops"]["load_to_postgis"]["inputs"]["manifest"]["value"][
            "batch_id"
        ]
        == valid_manifest_dict["batch_id"]
    )

    # Check tags including lane and archive key
    assert run_request.tags["batch_id"] == valid_manifest_dict["batch_id"]
    assert run_request.tags["uploader"] == valid_manifest_dict["uploader"]
    assert run_request.tags["intent"] == valid_manifest_dict["intent"]
    assert run_request.tags["manifest_key"] == manifest_key
    assert run_request.tags["lane"] == "ingest"
    assert run_request.tags["manifest_archive_key"] == f"archive/{manifest_key}"
    assert run_request.tags["operator"] == valid_manifest_dict["uploader"]
    assert run_request.tags["source"] == "unit-test"

    # Check archiving was called
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)

    assert mock_sensor_context.update_cursor.called


def test_tabular_intent_is_skipped_and_not_archived(
    mock_sensor_context, mock_minio_resource, valid_tabular_manifest_dict
):
    """Tabular intents are handled by tabular_sensor; legacy manifest_sensor should skip and not archive."""
    manifest_key = "manifests/batch_tabular.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = valid_tabular_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert results == []
    # Must not archive: tabular_sensor should still be able to see the manifest
    mock_minio_resource.move_to_archive.assert_not_called()
    assert mock_sensor_context.update_cursor.called


def test_join_intent_is_skipped_and_not_archived(
    mock_sensor_context,
    mock_minio_resource,
    valid_manifest_dict,
    valid_tabular_file_entry_dict,
):
    """Join intents are handled by join_sensor; legacy manifest_sensor should skip and not archive."""
    manifest_key = "manifests/batch_join.json"
    join_manifest = {
        **valid_manifest_dict,
        "intent": "join_datasets",
        "batch_id": "batch_join",
        "files": [],
        "metadata": {
            **valid_manifest_dict["metadata"],
            "join_config": {
                "spatial_dataset_id": "sa1_spatial_001",
                "tabular_dataset_id": "sa1_tabular_001",
                "left_key": "parcel_id",
                "right_key": "parcel_id",
                "how": "left",
            },
        },
    }
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = join_manifest

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert results == []
    mock_minio_resource.move_to_archive.assert_not_called()
    assert mock_sensor_context.update_cursor.called


# =============================================================================
# Test: Traffic Controller - default to ingest only
# =============================================================================


@patch.dict(os.environ, {}, clear=True)
def test_default_enabled_lanes_is_ingest_only(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    """Test that unset env var defaults to ingest lane only."""
    manifest_key = "manifests/batch_001.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    # Should process ingest lane manifest
    assert len(results) == 1
    assert results[0].tags["lane"] == "ingest"


# =============================================================================
# Test: Invalid manifest
# =============================================================================


@patch.dict(os.environ, {}, clear=False)
def test_invalid_manifest_logs_error_and_adds_to_cursor(
    mock_sensor_context, mock_minio_resource
):
    """Test that invalid manifest logs error, doesn't yield RunRequest, but adds to cursor."""
    manifest_key = "manifests/batch_001.json"
    invalid_manifest = {"batch_id": "test"}  # Missing required fields
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = invalid_manifest

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    # Should not yield RunRequest
    assert len(results) == 0
    # Should log error
    mock_sensor_context.log.error.assert_called()
    # Should update cursor (mark as processed, only try once)
    assert mock_sensor_context.update_cursor.called
    # Should archive invalid manifest
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)


# =============================================================================
# Test: Cursor tracking - JSON format
# =============================================================================


@patch.dict(os.environ, {}, clear=False)
def test_cursor_tracks_processed_manifests_json_format(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    """Test that cursor correctly tracks processed manifests in JSON format."""
    processed_key = "manifests/batch_001.json"
    new_key = "manifests/batch_002.json"

    # Set JSON cursor
    cursor_data = {"v": 1, "processed_keys": [processed_key], "max_keys": 500}
    mock_sensor_context.cursor = json.dumps(cursor_data)
    mock_minio_resource.list_manifests.return_value = [processed_key, new_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    # Should only process new manifest
    assert len(results) == 1
    assert isinstance(results[0], RunRequest)
    # Cursor should include both old and new
    assert mock_sensor_context.update_cursor.called
    cursor_str = mock_sensor_context.update_cursor.call_args[0][0]
    cursor_obj = json.loads(cursor_str)
    assert cursor_obj["v"] == 1
    assert processed_key in cursor_obj["processed_keys"]
    assert new_key in cursor_obj["processed_keys"]


# =============================================================================
# Test: Cursor migration from legacy format
# =============================================================================


@patch.dict(os.environ, {}, clear=False)
def test_cursor_migration_from_legacy_format(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    """Test that legacy comma-separated cursor is migrated to JSON format."""
    processed_key = "manifests/batch_001.json"
    new_key = "manifests/batch_002.json"

    # Set legacy comma-separated cursor
    mock_sensor_context.cursor = f"{processed_key},manifests/old.json"
    mock_minio_resource.list_manifests.return_value = [processed_key, new_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    # Should process new manifest
    assert len(results) == 1
    # Cursor should be migrated to JSON format
    assert mock_sensor_context.update_cursor.called
    cursor_str = mock_sensor_context.update_cursor.call_args[0][0]
    cursor_obj = json.loads(cursor_str)
    assert cursor_obj["v"] == 1
    assert isinstance(cursor_obj["processed_keys"], list)


# =============================================================================
# Test: Cursor ordering (non-lexicographic)
# =============================================================================


@patch.dict(os.environ, {}, clear=False)
def test_cursor_preserves_processing_order_not_lexicographic(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    """Test that cursor preserves processing order, not lexicographic order."""
    # Start with intentionally non-sorted cursor (z before a)
    processed_keys = ["manifests/z.json", "manifests/a.json"]
    new_key = "manifests/m.json"

    cursor_data = {"v": 1, "processed_keys": processed_keys, "max_keys": 500}
    mock_sensor_context.cursor = json.dumps(cursor_data)
    mock_minio_resource.list_manifests.return_value = processed_keys + [new_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    # Should process new manifest
    assert len(results) == 1
    # Updated cursor should preserve order: existing + new
    assert mock_sensor_context.update_cursor.called
    cursor_str = mock_sensor_context.update_cursor.call_args[0][0]
    cursor_obj = json.loads(cursor_str)
    expected_keys = ["manifests/z.json", "manifests/a.json", "manifests/m.json"]
    assert cursor_obj["processed_keys"] == expected_keys


# =============================================================================
# Test: Cursor bounding preserves tail
# =============================================================================


@patch.dict(os.environ, {}, clear=False)
def test_cursor_bounding_preserves_tail_processing_order(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    """Test that cursor bounding keeps the most recently processed keys (tail)."""
    # Create keys in known order, with last few being identifiable
    many_keys = [f"manifests/batch_{i:03d}.json" for i in range(600)]
    # Last few keys that should survive bounding
    tail_keys = many_keys[-10:]  # batch_590.json through batch_599.json
    new_key = "manifests/batch_new.json"

    # Set cursor with many keys
    cursor_data = {"v": 1, "processed_keys": many_keys, "max_keys": 500}
    mock_sensor_context.cursor = json.dumps(cursor_data)
    mock_minio_resource.list_manifests.return_value = many_keys + [new_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    # Should process new manifest
    assert len(results) == 1
    # Updated cursor should be bounded
    assert mock_sensor_context.update_cursor.called
    cursor_str = mock_sensor_context.update_cursor.call_args[0][0]
    cursor_obj = json.loads(cursor_str)
    # Should cap to max_keys (500)
    assert len(cursor_obj["processed_keys"]) == 500
    # Should preserve the tail of the original keys + new key
    # Original: 600 keys, add 1 new = 601 total, keep last 500
    # So: keys 101-599 (499 keys) + new_key (1 key) = 500 keys
    expected_tail = many_keys[101:] + [new_key]
    assert cursor_obj["processed_keys"] == expected_tail


# =============================================================================
# Test: Multiple manifests
# =============================================================================


@patch.dict(os.environ, {}, clear=False)
def test_multiple_manifests_yields_multiple_requests(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    """Test that multiple manifests yield multiple RunRequests."""
    keys = ["manifests/batch_001.json", "manifests/batch_002.json"]
    mock_minio_resource.list_manifests.return_value = keys
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 2
    assert all(isinstance(r, RunRequest) for r in results)
    # All should be archived
    assert mock_minio_resource.move_to_archive.call_count == 2


# =============================================================================
# Test: MinIO errors
# =============================================================================


def test_minio_error_handled_gracefully(mock_sensor_context, mock_minio_resource):
    """Test that MinIO errors yield SkipReason instead of crashing."""
    mock_minio_resource.list_manifests.side_effect = RuntimeError("Connection failed")

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
    mock_sensor_context.log.error.assert_called()


# =============================================================================
# Test: Individual manifest errors don't stop processing
# =============================================================================


@patch.dict(os.environ, {}, clear=False)
def test_individual_manifest_error_continues_processing(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    """Test that errors processing one manifest don't stop processing of others."""
    valid_key = "manifests/batch_001.json"
    error_key = "manifests/batch_002.json"

    mock_minio_resource.list_manifests.return_value = [valid_key, error_key]

    # First call returns valid manifest, second call raises error
    def get_manifest_side_effect(key):
        if key == error_key:
            raise RuntimeError("Download failed")
        return valid_manifest_dict

    mock_minio_resource.get_manifest.side_effect = get_manifest_side_effect

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    # Should process valid manifest
    assert len(results) == 1
    assert isinstance(results[0], RunRequest)
    # Should log error for failed manifest
    assert mock_sensor_context.log.error.call_count >= 1
    # Both should be added to cursor
    assert mock_sensor_context.update_cursor.called
    # Both should be archived
    assert mock_minio_resource.move_to_archive.call_count == 2


# =============================================================================
# Test: Archive failure doesn't break processing
# =============================================================================


@patch.dict(os.environ, {}, clear=False)
def test_archive_failure_logs_warning_but_continues(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    """Test that archive failure logs warning but still updates cursor."""
    manifest_key = "manifests/batch_001.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict
    mock_minio_resource.move_to_archive.side_effect = RuntimeError("Archive failed")

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    # Should still process manifest
    assert len(results) == 1
    # Should log warning about archive failure
    mock_sensor_context.log.warning.assert_called()
    # Should still update cursor
    assert mock_sensor_context.update_cursor.called
