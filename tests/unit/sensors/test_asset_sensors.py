"""
Unit tests for asset sensors (spatial_sensor, tabular_sensor, join_sensor).

These sensors are narrow: they only claim their specific intents and launch
asset-based jobs that configure `raw_manifest_json`.
"""

import pytest
from unittest.mock import Mock

from dagster import RunRequest, SkipReason

from services.dagster.etl_pipelines.resources import MinIOResource
from services.dagster.etl_pipelines.sensors.join_sensor import join_sensor
from services.dagster.etl_pipelines.sensors.spatial_sensor import spatial_sensor
from services.dagster.etl_pipelines.sensors.tabular_sensor import tabular_sensor


@pytest.fixture
def mock_minio_resource():
    resource = Mock(spec=MinIOResource)
    resource.move_to_archive = Mock()
    return resource


@pytest.fixture
def mock_sensor_context():
    context = Mock()
    context.cursor = None
    context.log = Mock()
    context.update_cursor = Mock()
    return context


def test_spatial_sensor_skips_when_no_manifests(mock_sensor_context, mock_minio_resource):
    mock_minio_resource.list_manifests.return_value = []
    results = list(spatial_sensor._raw_fn(mock_sensor_context, mock_minio_resource))
    assert len(results) == 1
    assert isinstance(results[0], SkipReason)


def test_tabular_sensor_emits_run_request_for_ingest_tabular(
    mock_sensor_context, mock_minio_resource, valid_tabular_manifest_dict
):
    manifest_key = "manifests/batch_tabular.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = valid_tabular_manifest_dict

    results = list(tabular_sensor._raw_fn(mock_sensor_context, mock_minio_resource))
    assert len(results) == 1
    assert isinstance(results[0], RunRequest)
    rr = results[0]
    # job_name is provided by the sensor decorator; RunRequest.job_name is typically None
    assert rr.job_name is None
    assert rr.partition_key is not None
    assert rr.run_config["ops"]["raw_manifest_json"]["config"]["manifest"]["batch_id"] == valid_tabular_manifest_dict["batch_id"]
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)


def test_join_sensor_requires_target_asset_id(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict, valid_tabular_file_entry_dict
):
    manifest_key = "manifests/batch_join.json"
    join_manifest = {
        **valid_manifest_dict,
        "intent": "join_datasets",
        "batch_id": "batch_join",
        "files": [valid_tabular_file_entry_dict],
        "metadata": {
            **valid_manifest_dict["metadata"],
            "join_config": {
                # target_asset_id intentionally missing
                "left_key": "parcel_id",
                "right_key": "parcel_id",
                "how": "left",
            },
        },
    }
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = join_manifest

    results = list(join_sensor._raw_fn(mock_sensor_context, mock_minio_resource))
    assert results == []
    # invalid join manifest is archived
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)
    assert mock_sensor_context.update_cursor.called


