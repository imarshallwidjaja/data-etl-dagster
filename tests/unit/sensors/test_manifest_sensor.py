"""
Unit tests for manifest_sensor smart router.

Tests sensor routing, gating, cursor migration, and archiving behavior with mocked
MinIOResource and SensorEvaluationContext.
"""

import json
from unittest.mock import Mock

import pytest
from dagster import RunRequest, SkipReason

from services.dagster.etl_pipelines.resources import MinIOResource
from services.dagster.etl_pipelines.sensors.manifest_sensor import manifest_sensor


# Access the raw function from the sensor
_manifest_sensor_fn = manifest_sensor._raw_fn


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def mock_minio_resource():
    """Create a mock MinIOResource."""
    resource = Mock(spec=MinIOResource)
    return resource


@pytest.fixture
def mock_sensor_context():
    """Create a mock SensorEvaluationContext."""
    context = Mock()
    context.cursor = None
    context.log = Mock()
    context.update_cursor = Mock()
    return context


# =============================================================================
# Tests
# =============================================================================

def test_no_manifests_skips(mock_sensor_context, mock_minio_resource):
    mock_minio_resource.list_manifests.return_value = []

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
    assert "No new manifests" in results[0].skip_message


def test_legacy_route_yields_run_request(mock_sensor_context, mock_minio_resource, valid_manifest_dict):
    manifest_key = "manifests/batch_001.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    request = results[0]
    assert isinstance(request, RunRequest)
    assert request.job_name == "ingest_job"
    assert request.run_key == f"legacy:{valid_manifest_dict['batch_id']}"
    assert request.tags["manifest_key"] == manifest_key
    assert request.tags["manifest_archive_key"] == f"archive/{manifest_key}"
    assert request.tags["route"] == "legacy"

    legacy_ops = request.run_config["ops"]["load_to_postgis"]["inputs"]["manifest"]["value"]
    assert legacy_ops["batch_id"] == valid_manifest_dict["batch_id"]

    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)
    cursor_payload = json.loads(mock_sensor_context.update_cursor.call_args[0][0])
    assert cursor_payload["v"] == 1
    assert manifest_key in cursor_payload["processed_keys"]


def test_tabular_route_disabled_by_default(mock_sensor_context, mock_minio_resource, valid_manifest_dict):
    manifest_key = "manifests/batch_001.json"
    valid_manifest_dict["intent"] = "ingest_tabular"

    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert results == []  # gated off
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)
    cursor_payload = json.loads(mock_sensor_context.update_cursor.call_args[0][0])
    assert manifest_key in cursor_payload["processed_keys"]


def test_tabular_route_enabled(monkeypatch, mock_sensor_context, mock_minio_resource, valid_manifest_dict):
    monkeypatch.setenv("MANIFEST_ROUTER_ENABLED_ROUTES", "legacy,tabular")
    manifest_key = "manifests/batch_001.json"
    valid_manifest_dict["intent"] = "ingest_tabular"

    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    request = results[0]
    assert request.job_name == "ingest_tabular_job"
    assert request.run_key == f"tabular:{valid_manifest_dict['batch_id']}"
    assert request.tags["route"] == "tabular"
    assert "tabular_ingest_placeholder" in request.run_config["ops"]

    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)


def test_join_route_requires_join_config(monkeypatch, mock_sensor_context, mock_minio_resource, valid_manifest_dict):
    monkeypatch.setenv("MANIFEST_ROUTER_ENABLED_ROUTES", "legacy,tabular,join")
    manifest_key = "manifests/batch_002.json"
    valid_manifest_dict["intent"] = "join_datasets"
    valid_manifest_dict["metadata"] = {
        "project": "ALPHA",
        "description": "No join config",
        "tags": {},
    }

    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert results == []  # validation error prevents run
    mock_sensor_context.log.error.assert_called()
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)


def test_join_route_enabled(monkeypatch, mock_sensor_context, mock_minio_resource, valid_manifest_dict):
    monkeypatch.setenv("MANIFEST_ROUTER_ENABLED_ROUTES", "legacy,tabular,join")
    manifest_key = "manifests/batch_003.json"
    valid_manifest_dict["intent"] = "join_datasets"

    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    request = results[0]
    assert request.job_name == "join_datasets_job"
    assert request.run_key == f"join:{valid_manifest_dict['batch_id']}"
    assert request.tags["route"] == "join"
    assert "join_datasets_placeholder" in request.run_config["ops"]


def test_cursor_migration_and_bounding(monkeypatch, mock_sensor_context, mock_minio_resource, valid_manifest_dict):
    monkeypatch.setenv("MANIFEST_ROUTER_ENABLED_ROUTES", "legacy")
    existing_cursor = json.dumps({"v": 1, "processed_keys": ["old1", "old2"], "max_keys": 2})
    mock_sensor_context.cursor = existing_cursor

    new_key = "manifests/new.json"
    mock_minio_resource.list_manifests.return_value = ["old1", "old2", new_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    cursor_payload = json.loads(mock_sensor_context.update_cursor.call_args[0][0])
    assert cursor_payload["processed_keys"] == ["old2", new_key]
    assert cursor_payload["max_keys"] == 2


def test_archive_failure_still_updates_cursor(mock_sensor_context, mock_minio_resource, valid_manifest_dict):
    manifest_key = "manifests/batch_004.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = valid_manifest_dict
    mock_minio_resource.move_to_archive.side_effect = RuntimeError("archive failed")

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    mock_sensor_context.log.warning.assert_called()
    cursor_payload = json.loads(mock_sensor_context.update_cursor.call_args[0][0])
    assert manifest_key in cursor_payload["processed_keys"]


def test_minio_error_handled_gracefully(mock_sensor_context, mock_minio_resource):
    mock_minio_resource.list_manifests.side_effect = RuntimeError("Connection failed")

    results = list(_manifest_sensor_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    assert isinstance(results[0], SkipReason)
    mock_sensor_context.log.error.assert_called()

