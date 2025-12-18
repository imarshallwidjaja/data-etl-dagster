"""Integration test: Sensor partition creation.

This test validates that sensors correctly create dynamic partitions before
creating RunRequests. This ensures sensors don't fail with DagsterUnknownPartitionError.

Tests:
1. Upload manifest to MinIO
2. Evaluate sensor manually (with real Dagster instance)
3. Verify partition was created
4. Verify RunRequest was created successfully with valid partition key
"""

import json
import time
from io import BytesIO
from typing import Optional
from uuid import uuid4

import pytest
from dagster import DagsterInstance
from minio import Minio

from libs.models import MinIOSettings
from services.dagster.etl_pipelines.partitions import dataset_partitions
from services.dagster.etl_pipelines.resources import MinIOResource
from services.dagster.etl_pipelines.sensors.tabular_sensor import tabular_sensor


pytestmark = [pytest.mark.integration]


@pytest.fixture
def dagster_instance():
    """Get ephemeral Dagster instance for sensor evaluation.
    
    Uses ephemeral instance for testing sensor logic without requiring
    DAGSTER_HOME to be set. The sensor will create partitions in this
    instance, which we can verify.
    """
    return DagsterInstance.ephemeral()


@pytest.fixture
def minio_settings():
    return MinIOSettings()


@pytest.fixture
def minio_client(minio_settings):
    client = Minio(
        minio_settings.endpoint,
        access_key=minio_settings.access_key,
        secret_key=minio_settings.secret_key,
        secure=minio_settings.use_ssl,
    )

    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            client.list_buckets()
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("MinIO did not become ready within timeout")

    for bucket in [minio_settings.landing_bucket, minio_settings.lake_bucket]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
    return client


@pytest.fixture
def minio_resource(minio_settings):
    """Create MinIOResource for sensor."""
    return MinIOResource(
        endpoint=minio_settings.endpoint,
        access_key=minio_settings.access_key,
        secret_key=minio_settings.secret_key,
        use_ssl=minio_settings.use_ssl,
        landing_bucket=minio_settings.landing_bucket,
        lake_bucket=minio_settings.lake_bucket,
    )


def _create_dummy_csv() -> bytes:
    """Create a simple CSV for testing."""
    return b"col_a,col_b,col_c\n1,2,3\n4,5,6\n"


def _upload_manifest_to_minio(
    minio_client: Minio,
    landing_bucket: str,
    manifest_key: str,
    manifest: dict,
) -> None:
    """Upload manifest JSON to MinIO."""
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
    data = BytesIO(manifest_bytes)
    minio_client.put_object(
        landing_bucket,
        manifest_key,
        data,
        length=len(manifest_bytes),
        content_type="application/json",
    )


def _upload_csv_to_landing_zone(
    minio_client: Minio,
    landing_bucket: str,
    object_key: str,
    csv_bytes: bytes,
) -> None:
    """Upload CSV file to landing zone."""
    data = BytesIO(csv_bytes)
    minio_client.put_object(
        landing_bucket,
        object_key,
        data,
        length=len(csv_bytes),
        content_type="text/csv",
    )


def _check_partition_exists(
    dagster_instance: DagsterInstance, partition_key: str
) -> bool:
    """Check if a dynamic partition exists."""
    partitions = dagster_instance.get_dynamic_partitions(
        partitions_def_name=dataset_partitions.name
    )
    return partition_key in partitions


def _cleanup_manifest(
    minio_client: Minio, landing_bucket: str, manifest_key: str
) -> None:
    """Remove manifest from MinIO."""
    try:
        minio_client.remove_object(landing_bucket, manifest_key)
    except Exception:
        pass


def _cleanup_archived_manifest(
    minio_client: Minio, landing_bucket: str, manifest_key: str
) -> None:
    """Remove archived manifest from MinIO."""
    archive_key = f"archive/{manifest_key}"
    try:
        minio_client.remove_object(landing_bucket, archive_key)
    except Exception:
        pass


class TestSensorPartitionCreation:
    """Test that sensors create partitions before RunRequests."""

    def test_tabular_sensor_creates_partition_before_run_request(
        self,
        dagster_instance: DagsterInstance,
        minio_client: Minio,
        minio_settings: MinIOSettings,
        minio_resource: MinIOResource,
    ):
        """
        Test that tabular_sensor creates dynamic partition before creating RunRequest.

        This test:
        1. Uploads a manifest with a specific dataset_id to MinIO
        2. Evaluates the sensor manually
        3. Verifies the partition was created
        4. Verifies the RunRequest was created successfully
        """
        # Generate unique test identifiers
        batch_id = f"sensor_test_{uuid4().hex[:12]}"
        dataset_id = f"test_dataset_{uuid4().hex[:12]}"
        manifest_key = f"manifests/{batch_id}.json"
        csv_key = f"test/{batch_id}/data.csv"

        # Create manifest with explicit dataset_id
        manifest = {
            "batch_id": batch_id,
            "uploader": "sensor_test",
            "intent": "ingest_tabular",
            "files": [
                {
                    "path": f"s3://landing-zone/{csv_key}",
                    "type": "tabular",
                    "format": "CSV",
                }
            ],
            "metadata": {
                "project": "SENSOR_TEST",
                "description": "Test sensor partition creation",
                "tags": {
                    "dataset_id": dataset_id,
                    "source": "sensor_test",
                },
            },
        }

        try:
            # 1. Upload CSV and manifest to landing zone
            csv_bytes = _create_dummy_csv()
            _upload_csv_to_landing_zone(
                minio_client, minio_settings.landing_bucket, csv_key, csv_bytes
            )
            _upload_manifest_to_minio(
                minio_client,
                minio_settings.landing_bucket,
                manifest_key,
                manifest,
            )

            # 2. Verify partition does NOT exist yet
            assert not _check_partition_exists(
                dagster_instance, dataset_id
            ), "Partition should not exist before sensor evaluation"

            # 3. Create sensor context with real Dagster instance
            from unittest.mock import Mock

            context = Mock()
            context.instance = dagster_instance
            context.cursor = None
            context.log = Mock()
            context.log.info = Mock()
            context.log.error = Mock()
            context.log.warning = Mock()
            context.update_cursor = Mock()

            # 4. Evaluate sensor
            results = list(tabular_sensor._raw_fn(context, minio_resource))

            # 5. Filter results to find our test manifest's RunRequest
            # (sensor may process other manifests from previous test runs)
            from dagster import RunRequest

            test_run_requests = [
                r
                for r in results
                if isinstance(r, RunRequest)
                and r.tags.get("manifest_key") == manifest_key
            ]
            assert (
                len(test_run_requests) == 1
            ), f"Expected 1 RunRequest for our test manifest, got {len(test_run_requests)}. All results: {[r.tags.get('manifest_key') for r in results if isinstance(r, RunRequest)]}"

            run_request = test_run_requests[0]
            assert (
                run_request.partition_key == dataset_id
            ), f"Expected partition_key={dataset_id}, got {run_request.partition_key}"

            # 6. Verify partition was created
            assert _check_partition_exists(
                dagster_instance, dataset_id
            ), f"Partition {dataset_id} should exist after sensor evaluation"

            # 7. Verify RunRequest is valid (can be used to launch a run)
            assert run_request.run_key is not None
            assert run_request.run_config is not None
            assert "ops" in run_request.run_config
            assert "raw_manifest_json" in run_request.run_config["ops"]

        finally:
            # Cleanup
            _cleanup_manifest(minio_client, minio_settings.landing_bucket, manifest_key)
            _cleanup_archived_manifest(
                minio_client, minio_settings.landing_bucket, manifest_key
            )
            try:
                minio_client.remove_object(minio_settings.landing_bucket, csv_key)
            except Exception:
                pass

            # Clean up partition (optional, but good for test isolation)
            try:
                dagster_instance.delete_dynamic_partition(
                    partitions_def_name=dataset_partitions.name,
                    partition_key=dataset_id,
                )
            except Exception:
                pass  # Partition may not exist or may have been cleaned up

