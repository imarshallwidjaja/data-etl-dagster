"""Integration test: Sensor-triggered E2E pipeline tests.

This test validates the complete sensor→job→materialization flow:
1. Upload data file to landing-zone
2. Upload manifest to manifests/ for sensor detection
3. Evaluate sensor → get RunRequest
4. Launch job via GraphQL using that RunRequest's config
5. Poll for completion
6. Verify MongoDB asset and data lake output
"""

import json
from io import BytesIO
from pathlib import Path
from typing import Optional
from uuid import uuid4

import pytest
from dagster import DagsterInstance, RunRequest
from minio import Minio
from pymongo import MongoClient

from libs.models import MinIOSettings, MongoSettings
from services.dagster.etl_pipelines.partitions import dataset_partitions
from services.dagster.etl_pipelines.resources import MinIOResource
from services.dagster.etl_pipelines.sensors.spatial_sensor import spatial_sensor
from services.dagster.etl_pipelines.sensors.tabular_sensor import tabular_sensor

from .helpers import (
    DagsterGraphQLClient,
    assert_datalake_object_exists,
    assert_mongodb_asset_exists,
    delete_dynamic_partition,
    format_error_details,
    poll_run_to_completion,
)


pytestmark = [pytest.mark.integration, pytest.mark.e2e]

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "asset_plans"
SPATIAL_DATASET_PATH = FIXTURES_DIR / "e2e_sample_sa1_data.json"
TABULAR_DATASET_PATH = FIXTURES_DIR / "e2e_sample_table_data.csv"
SPATIAL_MANIFEST_TEMPLATE = FIXTURES_DIR / "e2e_spatial_manifest.json"
TABULAR_MANIFEST_TEMPLATE = FIXTURES_DIR / "e2e_tabular_manifest.json"


@pytest.fixture
def dagster_instance():
    return DagsterInstance.ephemeral()


@pytest.fixture
def minio_resource(minio_settings):
    return MinIOResource(
        endpoint=minio_settings.endpoint,
        access_key=minio_settings.access_key,
        secret_key=minio_settings.secret_key,
        use_ssl=minio_settings.use_ssl,
        landing_bucket=minio_settings.landing_bucket,
        lake_bucket=minio_settings.lake_bucket,
    )


def _load_spatial_dataset() -> bytes:
    return SPATIAL_DATASET_PATH.read_bytes()


def _load_tabular_dataset() -> bytes:
    return TABULAR_DATASET_PATH.read_bytes()


def _load_manifest_template(
    template_path: Path, uuid: str, batch_id: str, data_key: str
) -> dict:
    template_text = template_path.read_text()
    substituted = template_text.replace("${UUID}", uuid).replace(
        "${BATCH_ID}", batch_id
    )
    manifest = json.loads(substituted)
    manifest["files"][0]["path"] = f"s3://landing-zone/{data_key}"
    return manifest


def _upload_bytes(
    minio_client: Minio,
    bucket: str,
    object_key: str,
    data_bytes: bytes,
    content_type: str,
) -> None:
    data = BytesIO(data_bytes)
    minio_client.put_object(
        bucket,
        object_key,
        data,
        length=len(data_bytes),
        content_type=content_type,
    )


def _upload_manifest(
    minio_client: Minio,
    bucket: str,
    manifest_key: str,
    manifest: dict,
) -> None:
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
    _upload_bytes(
        minio_client, bucket, manifest_key, manifest_bytes, "application/json"
    )


def _evaluate_sensor(
    sensor_fn, dagster_instance, minio_resource, expected_manifest_key: str
):
    from unittest.mock import Mock

    context = Mock()
    context.instance = dagster_instance
    context.cursor = None
    context.log = Mock()
    context.log.info = Mock()
    context.log.error = Mock()
    context.log.warning = Mock()
    context.update_cursor = Mock()

    results = list(sensor_fn._raw_fn(context, minio_resource))

    run_requests = [
        r
        for r in results
        if isinstance(r, RunRequest)
        and r.tags.get("manifest_key") == expected_manifest_key
    ]

    if len(run_requests) != 1:
        raise AssertionError(
            f"Expected 1 RunRequest for manifest {expected_manifest_key}, got {len(run_requests)}"
        )

    return run_requests[0]


def _launch_job_from_run_request(
    dagster_client: DagsterGraphQLClient, job_name: str, run_request: RunRequest
) -> str:
    mutation = """
    mutation LaunchRun($executionParams: ExecutionParams!) {
        launchRun(executionParams: $executionParams) {
            __typename
            ... on LaunchRunSuccess {
                run {
                    runId
                }
            }
            ... on PythonError {
                message
                stack
            }
            ... on InvalidSubsetError {
                message
            }
            ... on InvalidOutputError {
                stepKey
                invalidOutputName
            }
        }
    }
    """

    variables = {
        "executionParams": {
            "selector": {
                "repositoryLocationName": "etl_pipelines",
                "repositoryName": "__repository__",
                "jobName": job_name,
            },
            "runConfigData": run_request.run_config,
            "executionMetadata": {
                "tags": [
                    {"key": "dagster/partition", "value": run_request.partition_key}
                ]
            },
        }
    }

    result = dagster_client.query(mutation, variables)

    if "errors" in result:
        raise RuntimeError(f"GraphQL errors: {result['errors']}")

    data = result.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected response: {result}")

    launch_result = data.get("launchRun")
    if not isinstance(launch_result, dict):
        raise RuntimeError(f"Unexpected launch result: {data}")

    if launch_result.get("__typename") != "LaunchRunSuccess":
        raise RuntimeError(f"Launch failed: {launch_result}")

    run = launch_result.get("run")
    if not isinstance(run, dict):
        raise RuntimeError(f"Missing run in result: {launch_result}")

    run_id = run.get("runId")
    if not isinstance(run_id, str):
        raise RuntimeError(f"Missing runId: {run}")

    return run_id


def _cleanup(
    minio_client: Minio,
    minio_settings: MinIOSettings,
    mongo_client: MongoClient,
    mongo_settings: MongoSettings,
    dagster_client: DagsterGraphQLClient,
    landing_key: str,
    manifest_key: str,
    partition_key: str,
    asset_doc: Optional[dict],
) -> None:
    try:
        minio_client.remove_object(minio_settings.landing_bucket, landing_key)
    except Exception:
        pass

    try:
        minio_client.remove_object(minio_settings.landing_bucket, manifest_key)
    except Exception:
        pass

    try:
        minio_client.remove_object(
            minio_settings.landing_bucket, f"archive/{manifest_key}"
        )
    except Exception:
        pass

    if asset_doc and asset_doc.get("s3_key"):
        try:
            minio_client.remove_object(minio_settings.lake_bucket, asset_doc["s3_key"])
        except Exception:
            pass

    if asset_doc:
        try:
            db = mongo_client[mongo_settings.database]
            db["assets"].delete_one({"_id": asset_doc["_id"]})
        except Exception:
            pass

    delete_dynamic_partition(dagster_client, partition_key, strict=True)


class TestSensorToJobE2E:
    def test_tabular_sensor_triggers_job_e2e(
        self,
        dagster_client,
        dagster_instance,
        minio_client,
        minio_settings,
        minio_resource,
        mongo_client,
        mongo_settings,
    ):
        test_uuid = uuid4().hex[:8]
        batch_id = f"sensor_e2e_tabular_{test_uuid}"
        csv_key = f"e2e/{batch_id}/data.csv"
        manifest_key = f"manifests/{batch_id}.json"

        manifest = _load_manifest_template(
            template_path=TABULAR_MANIFEST_TEMPLATE,
            uuid=test_uuid,
            batch_id=batch_id,
            data_key=csv_key,
        )
        dataset_id = manifest["metadata"]["tags"]["dataset_id"]

        asset_doc = None
        run_id = None

        try:
            csv_bytes = _load_tabular_dataset()
            _upload_bytes(
                minio_client,
                minio_settings.landing_bucket,
                csv_key,
                csv_bytes,
                "text/csv",
            )

            _upload_manifest(
                minio_client,
                minio_settings.landing_bucket,
                manifest_key,
                manifest,
            )

            run_request = _evaluate_sensor(
                tabular_sensor, dagster_instance, minio_resource, manifest_key
            )
            assert run_request.partition_key == dataset_id

            run_id = _launch_job_from_run_request(
                dagster_client, "tabular_asset_job", run_request
            )

            status, error_details = poll_run_to_completion(dagster_client, run_id)
            if status != "SUCCESS":
                pytest.fail(
                    f"tabular_asset_job failed: {status}.{format_error_details(error_details)}"
                )

            asset_doc = assert_mongodb_asset_exists(
                mongo_client, mongo_settings, run_id
            )
            assert "dataset_id" in asset_doc, "Asset should have dataset_id"

            s3_key = asset_doc.get("s3_key")
            assert isinstance(s3_key, str), "Asset missing s3_key"
            assert_datalake_object_exists(
                minio_client, minio_settings.lake_bucket, s3_key
            )

        finally:
            _cleanup(
                minio_client,
                minio_settings,
                mongo_client,
                mongo_settings,
                dagster_client,
                csv_key,
                manifest_key,
                dataset_id,
                asset_doc,
            )

    def test_spatial_sensor_triggers_job_e2e(
        self,
        dagster_client,
        dagster_instance,
        minio_client,
        minio_settings,
        minio_resource,
        mongo_client,
        mongo_settings,
    ):
        test_uuid = uuid4().hex[:8]
        batch_id = f"sensor_e2e_spatial_{test_uuid}"
        geojson_key = f"e2e/{batch_id}/data.geojson"
        manifest_key = f"manifests/{batch_id}.json"

        manifest = _load_manifest_template(
            template_path=SPATIAL_MANIFEST_TEMPLATE,
            uuid=test_uuid,
            batch_id=batch_id,
            data_key=geojson_key,
        )
        dataset_id = manifest["metadata"]["tags"]["dataset_id"]

        asset_doc = None
        run_id = None

        try:
            geojson_bytes = _load_spatial_dataset()
            _upload_bytes(
                minio_client,
                minio_settings.landing_bucket,
                geojson_key,
                geojson_bytes,
                "application/geo+json",
            )

            _upload_manifest(
                minio_client,
                minio_settings.landing_bucket,
                manifest_key,
                manifest,
            )

            run_request = _evaluate_sensor(
                spatial_sensor, dagster_instance, minio_resource, manifest_key
            )
            assert run_request.partition_key == dataset_id

            run_id = _launch_job_from_run_request(
                dagster_client, "spatial_asset_job", run_request
            )

            status, error_details = poll_run_to_completion(dagster_client, run_id)
            if status != "SUCCESS":
                pytest.fail(
                    f"spatial_asset_job failed: {status}.{format_error_details(error_details)}"
                )

            asset_doc = assert_mongodb_asset_exists(
                mongo_client, mongo_settings, run_id
            )
            assert "dataset_id" in asset_doc, "Asset should have dataset_id"

            s3_key = asset_doc.get("s3_key")
            assert isinstance(s3_key, str), "Asset missing s3_key"
            assert_datalake_object_exists(
                minio_client, minio_settings.lake_bucket, s3_key
            )

        finally:
            _cleanup(
                minio_client,
                minio_settings,
                mongo_client,
                mongo_settings,
                dagster_client,
                geojson_key,
                manifest_key,
                dataset_id,
                asset_doc,
            )
