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
    # Mock instance for dynamic partition management
    context.instance = Mock()
    context.instance.add_dynamic_partitions = Mock()
    return context


def test_spatial_sensor_skips_when_no_manifests(
    mock_sensor_context, mock_minio_resource
):
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
    assert (
        rr.run_config["ops"]["raw_manifest_json"]["config"]["manifest"]["batch_id"]
        == valid_tabular_manifest_dict["batch_id"]
    )
    # Verify partition was created before RunRequest
    mock_sensor_context.instance.add_dynamic_partitions.assert_called_once()
    call_args = mock_sensor_context.instance.add_dynamic_partitions.call_args
    assert call_args[1]["partitions_def_name"] == "dataset_id"
    assert call_args[1]["partition_keys"] == [rr.partition_key]
    mock_minio_resource.move_to_archive.assert_called_once_with(manifest_key)
    assert rr.tags["operator"] == valid_tabular_manifest_dict["uploader"]
    assert rr.tags["source"] == "unit-test"


def test_join_sensor_requires_target_asset_id(
    mock_sensor_context,
    mock_minio_resource,
    valid_manifest_dict,
    valid_tabular_file_entry_dict,
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


def test_spatial_sensor_includes_operator_and_source_tags(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    spatial_manifest = {
        **valid_manifest_dict,
        "intent": "ingest_vector",
        "batch_id": "batch_spatial_001",
    }
    manifest_key = "manifests/batch_spatial_001.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = spatial_manifest

    results = list(spatial_sensor._raw_fn(mock_sensor_context, mock_minio_resource))
    assert len(results) == 1
    rr = results[0]
    assert rr.tags["operator"] == spatial_manifest["uploader"]
    assert rr.tags["source"] == "unit-test"


def test_spatial_sensor_includes_testing_tag(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    spatial_manifest = {
        **valid_manifest_dict,
        "intent": "ingest_vector",
        "batch_id": "batch_spatial_testing",
        "metadata": {
            **valid_manifest_dict["metadata"],
            "tags": {**valid_manifest_dict["metadata"]["tags"], "testing": True},
        },
    }
    manifest_key = "manifests/batch_spatial_testing.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = spatial_manifest

    results = list(spatial_sensor._raw_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    rr = results[0]
    assert rr.tags["testing"] == "true"


def test_join_sensor_includes_operator_and_source_tags(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    join_manifest = {
        **valid_manifest_dict,
        "intent": "join_datasets",
        "batch_id": "batch_join_001",
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
    manifest_key = "manifests/batch_join_001.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = join_manifest

    results = list(join_sensor._raw_fn(mock_sensor_context, mock_minio_resource))
    assert len(results) == 1
    rr = results[0]
    assert rr.tags["operator"] == join_manifest["uploader"]
    assert rr.tags["source"] == "unit-test"


def test_join_sensor_includes_testing_tag(
    mock_sensor_context, mock_minio_resource, valid_manifest_dict
):
    join_manifest = {
        **valid_manifest_dict,
        "intent": "join_datasets",
        "batch_id": "batch_join_testing",
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
            "tags": {**valid_manifest_dict["metadata"]["tags"], "testing": True},
        },
    }
    manifest_key = "manifests/batch_join_testing.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = join_manifest

    results = list(join_sensor._raw_fn(mock_sensor_context, mock_minio_resource))

    assert len(results) == 1
    rr = results[0]
    assert rr.tags["testing"] == "true"


def test_source_tag_prefers_ingestion_source_over_source(
    mock_sensor_context, mock_minio_resource, valid_tabular_manifest_dict
):
    manifest_with_ingestion_source = {
        **valid_tabular_manifest_dict,
        "metadata": {
            **valid_tabular_manifest_dict["metadata"],
            "tags": {
                "ingestion_source": "api_upload",
                "source": "unit-test",
            },
        },
    }
    manifest_key = "manifests/batch_ingestion_source.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = manifest_with_ingestion_source

    results = list(tabular_sensor._raw_fn(mock_sensor_context, mock_minio_resource))
    assert len(results) == 1
    rr = results[0]
    assert rr.tags["source"] == "api_upload"


def test_tabular_sensor_includes_testing_tag(
    mock_sensor_context, mock_minio_resource, valid_tabular_manifest_dict
):
    manifest_with_testing = {
        **valid_tabular_manifest_dict,
        "metadata": {
            **valid_tabular_manifest_dict["metadata"],
            "tags": {
                **valid_tabular_manifest_dict["metadata"]["tags"],
                "testing": True,
            },
        },
    }
    manifest_key = "manifests/batch_tabular_testing.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = manifest_with_testing

    results = list(tabular_sensor._raw_fn(mock_sensor_context, mock_minio_resource))
    assert len(results) == 1
    rr = results[0]
    assert rr.tags["testing"] == "true"


def test_source_tag_defaults_to_unknown_when_no_tags(
    mock_sensor_context, mock_minio_resource, valid_tabular_manifest_dict
):
    manifest_no_source = {
        **valid_tabular_manifest_dict,
        "metadata": {
            **valid_tabular_manifest_dict["metadata"],
            "tags": {},
        },
    }
    manifest_key = "manifests/batch_no_source.json"
    mock_minio_resource.list_manifests.return_value = [manifest_key]
    mock_minio_resource.get_manifest.return_value = manifest_no_source

    results = list(tabular_sensor._raw_fn(mock_sensor_context, mock_minio_resource))
    assert len(results) == 1
    rr = results[0]
    assert rr.tags["source"] == "unknown"
