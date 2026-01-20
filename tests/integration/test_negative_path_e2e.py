"""Integration tests: Negative-path E2E tests for failure scenarios.

These tests validate that the system handles failures correctly:
1. Invalid manifests are archived (not retried forever)
2. Join failures result in proper status
"""

from __future__ import annotations

import json
import time
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import pytest
from dagster import DagsterInstance
from minio.error import S3Error

from libs.models import MinIOSettings
from services.dagster.etl_pipelines.resources import MinIOResource
from services.dagster.etl_pipelines.sensors.spatial_sensor import spatial_sensor

from .helpers import (
    DagsterGraphQLClient,
    add_dynamic_partition,
    assert_mongodb_asset_exists,
    cleanup_dynamic_partitions,
    cleanup_minio_object,
    cleanup_mongodb_asset,
    cleanup_mongodb_lineage,
    format_error_details,
    poll_run_to_completion,
    upload_bytes_to_minio,
)


pytestmark = [pytest.mark.integration, pytest.mark.e2e]

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "asset_plans"
SPATIAL_DATASET_PATH = FIXTURES_DIR / "e2e_sample_sa1_data.json"
TABULAR_DATASET_PATH = FIXTURES_DIR / "e2e_sample_table_data.csv"


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


def _create_invalid_manifest() -> dict:
    """Create an invalid manifest missing required metadata fields."""
    return {
        "batch_id": f"invalid_{uuid4().hex[:8]}",
        "uploader": "e2e_test",
        "intent": "ingest_vector",
        "files": [
            {
                "path": "s3://landing-zone/fake/data.geojson",
                "type": "vector",
                "format": "GeoJSON",
            }
        ],
        # Missing required metadata fields: title, description, source, license, attribution
        "metadata": {"tags": {"dataset_id": f"invalid_{uuid4().hex[:8]}"}},
    }


def _evaluate_sensor_for_manifest(
    sensor_fn, dagster_instance, minio_resource, manifest_key: str
) -> None:
    """Evaluate sensor to process a specific manifest (trigger archiving)."""
    from unittest.mock import Mock

    context = Mock()
    context.instance = dagster_instance
    context.cursor = None
    context.log = Mock()
    context.log.info = Mock()
    context.log.error = Mock()
    context.log.warning = Mock()
    context.update_cursor = Mock()

    # Consume generator to execute sensor logic
    list(sensor_fn._raw_fn(context, minio_resource))


def _assert_manifest_archived(
    minio_client, minio_settings: MinIOSettings, manifest_key: str
) -> None:
    """Assert that a manifest has been archived."""
    archive_key = f"archive/{manifest_key}"
    try:
        stat = minio_client.stat_object(minio_settings.landing_bucket, archive_key)
        assert stat.size is not None and stat.size > 0, (
            f"Archived manifest {archive_key} has zero size"
        )
    except S3Error as e:
        raise AssertionError(f"Manifest was not archived to {archive_key}: {e}")


def _launch_job_with_run_config(
    dagster_client: DagsterGraphQLClient,
    *,
    job_name: str,
    run_config: dict,
    partition_key: str | None = None,
) -> str:
    """Launch a Dagster job via GraphQL and return run_id."""
    launch_query = """
    mutation LaunchRun(
        $repositoryLocationName: String!
        $repositoryName: String!
        $jobName: String!
        $runConfigData: RunConfigData
        $executionMetadata: ExecutionMetadata
    ) {
        launchRun(
            executionParams: {
                selector: {
                    repositoryLocationName: $repositoryLocationName
                    repositoryName: $repositoryName
                    pipelineName: $jobName
                }
                runConfigData: $runConfigData
                executionMetadata: $executionMetadata
            }
        ) {
            ... on LaunchRunSuccess { run { runId status } }
            ... on PipelineNotFoundError { message }
            ... on RunConfigValidationInvalid { errors { message } }
            ... on PythonError { message }
        }
    }
    """

    execution_metadata: dict = {"tags": []}
    if partition_key:
        execution_metadata["tags"].append(
            {"key": "dagster/partition", "value": partition_key}
        )

    variables = {
        "repositoryLocationName": "etl_pipelines",
        "repositoryName": "__repository__",
        "jobName": job_name,
        "runConfigData": run_config,
        "executionMetadata": execution_metadata,
    }
    result = dagster_client.query(launch_query, variables=variables, timeout=10)
    assert "errors" not in result, f"Failed to launch job: {result.get('errors')}"
    launch_response = result["data"]["launchRun"]
    assert "run" in launch_response, (
        f"Job launch failed: {launch_response.get('message', 'Unknown error')}"
    )
    return launch_response["run"]["runId"]


class TestInvalidManifestArchived:
    """Test that invalid manifests are archived (not retried forever)."""

    def test_invalid_manifest_is_archived_by_sensor(
        self,
        dagster_instance,
        minio_client,
        minio_settings,
        minio_resource,
    ):
        """Invalid manifest should be archived after sensor processes it."""
        test_uuid = uuid4().hex[:8]
        manifest_key = f"manifests/invalid_e2e_{test_uuid}.json"

        # Create invalid manifest (missing required metadata fields)
        invalid_manifest = _create_invalid_manifest()
        manifest_bytes = json.dumps(invalid_manifest, indent=2).encode("utf-8")

        try:
            # Upload invalid manifest
            upload_bytes_to_minio(
                minio_client,
                minio_settings.landing_bucket,
                manifest_key,
                manifest_bytes,
                "application/json",
            )

            # Evaluate sensor - it should detect invalid manifest and archive it
            _evaluate_sensor_for_manifest(
                spatial_sensor, dagster_instance, minio_resource, manifest_key
            )

            # Assert: manifest was archived (proves it won't be retried forever)
            _assert_manifest_archived(minio_client, minio_settings, manifest_key)

        finally:
            # Cleanup: remove both original and archived manifest
            cleanup_minio_object(
                minio_client, minio_settings.landing_bucket, manifest_key
            )
            cleanup_minio_object(
                minio_client, minio_settings.landing_bucket, f"archive/{manifest_key}"
            )


class TestJoinFailureCleanup:
    """Test that join failures result in proper status."""

    def test_join_failure_mismatched_key(
        self,
        dagster_client,
        minio_client,
        minio_settings,
        mongo_client,
        mongo_settings,
    ):
        """Join with mismatched key should fail."""
        batch_id = f"e2e_join_fail_{uuid4().hex[:12]}"
        spatial_partition_key = f"spatial_fail_{batch_id}"
        join_partition_key = f"joined_fail_{batch_id}"

        spatial_object_key = f"e2e/{batch_id}/spatial.json"

        spatial_run_id: str | None = None
        join_run_id: str | None = None
        spatial_asset_doc: dict | None = None
        created_partitions: set[str] = set()
        test_error: BaseException | None = None

        try:
            # 1) Create spatial parent via spatial_asset_job
            spatial_dataset_bytes = SPATIAL_DATASET_PATH.read_bytes()

            spatial_manifest = {
                "batch_id": batch_id,
                "uploader": "e2e_test",
                "intent": "ingest_vector",
                "files": [
                    {
                        "path": f"s3://landing-zone/{spatial_object_key}",
                        "type": "vector",
                        "format": "GeoJSON",
                    }
                ],
                "metadata": {
                    "title": "Spatial Parent for Join Failure Test",
                    "description": "Spatial parent dataset for join failure test",
                    "keywords": ["test", "spatial", "join", "failure"],
                    "source": "E2E Test Suite",
                    "license": "MIT",
                    "attribution": "Test Runner",
                    "project": "E2E_JOIN_FAILURE_TEST",
                    "tags": {"dataset_id": spatial_partition_key},
                },
            }

            upload_bytes_to_minio(
                minio_client,
                minio_settings.landing_bucket,
                spatial_object_key,
                spatial_dataset_bytes,
                "application/json",
            )

            add_dynamic_partition(dagster_client, spatial_partition_key)
            created_partitions.add(spatial_partition_key)

            spatial_run_id = _launch_job_with_run_config(
                dagster_client,
                job_name="spatial_asset_job",
                run_config={
                    "ops": {
                        "raw_manifest_json": {"config": {"manifest": spatial_manifest}}
                    }
                },
                partition_key=spatial_partition_key,
            )
            status, error_details = poll_run_to_completion(
                dagster_client, spatial_run_id
            )
            if status != "SUCCESS":
                pytest.fail(
                    f"spatial_asset_job failed: {status}.{format_error_details(error_details)}"
                )

            spatial_asset_doc = assert_mongodb_asset_exists(
                mongo_client, mongo_settings, spatial_run_id
            )
            spatial_dataset_id = spatial_asset_doc["dataset_id"]

            # 2) Create join manifest with MISMATCHED join key (will fail)
            # Use a non-existent tabular_dataset_id to trigger failure
            join_manifest = {
                "batch_id": f"join_fail_{batch_id}",
                "uploader": "e2e_test",
                "intent": "join_datasets",
                "files": [],
                "metadata": {
                    "title": "Join Failure Test",
                    "description": "Join that should fail due to missing tabular asset",
                    "keywords": ["join", "failure", "test"],
                    "source": "E2E Test Suite",
                    "license": "MIT",
                    "attribution": "Test Runner",
                    "tags": {"dataset_id": join_partition_key},
                    "join_config": {
                        "spatial_dataset_id": spatial_dataset_id,
                        # Non-existent tabular dataset - will cause join to fail
                        "tabular_dataset_id": f"nonexistent_{uuid4().hex[:12]}",
                        "left_key": "sa1_code21",
                        "right_key": "MISMATCHED_KEY_DOES_NOT_EXIST",
                        "how": "left",
                    },
                },
            }

            add_dynamic_partition(dagster_client, join_partition_key)
            created_partitions.add(join_partition_key)

            join_run_id = _launch_job_with_run_config(
                dagster_client,
                job_name="join_asset_job",
                run_config={
                    "ops": {
                        "raw_manifest_json": {"config": {"manifest": join_manifest}}
                    }
                },
                partition_key=join_partition_key,
            )

            # 3) Poll for completion - expect FAILURE
            status, error_details = poll_run_to_completion(dagster_client, join_run_id)

            # Assert: job should have failed
            assert status == "FAILURE", (
                f"Expected join_asset_job to fail, but got status: {status}"
            )

        except BaseException as e:
            test_error = e
            raise

        finally:
            # Cleanup
            cleanup_minio_object(
                minio_client, minio_settings.landing_bucket, spatial_object_key
            )

            db = mongo_client[mongo_settings.database]
            asset_ids = []
            if spatial_asset_doc:
                asset_ids.append(spatial_asset_doc["_id"])
                cleanup_minio_object(
                    minio_client,
                    minio_settings.lake_bucket,
                    spatial_asset_doc.get("s3_key", ""),
                )
                cleanup_mongodb_asset(mongo_client, mongo_settings, spatial_asset_doc)

            cleanup_mongodb_lineage(mongo_client, mongo_settings, asset_ids)
            cleanup_dynamic_partitions(
                dagster_client, created_partitions, original_error=test_error
            )
