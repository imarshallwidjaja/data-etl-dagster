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
import time
from io import BytesIO
from pathlib import Path
from typing import Optional
from uuid import uuid4

import pytest
import requests
from dagster import DagsterInstance, RunRequest
from minio import Minio
from pymongo import MongoClient

from libs.models import MinIOSettings, MongoSettings
from services.dagster.etl_pipelines.partitions import dataset_partitions
from services.dagster.etl_pipelines.resources import MinIOResource
from services.dagster.etl_pipelines.sensors.spatial_sensor import spatial_sensor
from services.dagster.etl_pipelines.sensors.tabular_sensor import tabular_sensor


pytestmark = [pytest.mark.integration, pytest.mark.e2e]

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "asset_plans"
SPATIAL_DATASET_PATH = FIXTURES_DIR / "e2e_sample_sa1_data.json"
TABULAR_DATASET_PATH = FIXTURES_DIR / "e2e_sample_table_data.csv"
SPATIAL_MANIFEST_TEMPLATE = FIXTURES_DIR / "e2e_spatial_manifest.json"
TABULAR_MANIFEST_TEMPLATE = FIXTURES_DIR / "e2e_tabular_manifest.json"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def dagster_graphql_url():
    return "http://localhost:3000/graphql"


@pytest.fixture
def dagster_instance():
    """Ephemeral Dagster instance for sensor evaluation."""
    return DagsterInstance.ephemeral()


@pytest.fixture
def dagster_client(dagster_graphql_url):
    """GraphQL client for Dagster."""

    class DagsterGraphQLClient:
        def __init__(self, url: str):
            self.url = url

        def query(
            self, query_str: str, variables: Optional[dict] = None, timeout: int = 30
        ) -> dict:
            response = requests.post(
                self.url,
                json={"query": query_str, "variables": variables or {}},
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()

    # Wait for GraphQL to be ready
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            client = DagsterGraphQLClient(dagster_graphql_url)
            client.query("{ __typename }")
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("Dagster GraphQL did not become ready within timeout")

    return client


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
    """MinIOResource for sensor evaluation."""
    return MinIOResource(
        endpoint=minio_settings.endpoint,
        access_key=minio_settings.access_key,
        secret_key=minio_settings.secret_key,
        use_ssl=minio_settings.use_ssl,
        landing_bucket=minio_settings.landing_bucket,
        lake_bucket=minio_settings.lake_bucket,
    )


@pytest.fixture
def mongo_settings():
    return MongoSettings()


@pytest.fixture
def mongo_client(mongo_settings):
    return MongoClient(mongo_settings.connection_string)


# =============================================================================
# Helper Functions
# =============================================================================


def _load_spatial_dataset() -> bytes:
    """Load sample GeoJSON from fixtures."""
    return SPATIAL_DATASET_PATH.read_bytes()


def _load_tabular_dataset() -> bytes:
    """Load sample CSV from fixtures."""
    return TABULAR_DATASET_PATH.read_bytes()


def _load_manifest_template(
    template_path: Path, uuid: str, batch_id: str, data_key: str
) -> dict:
    """Load manifest from fixture template file and substitute placeholders.

    Args:
        template_path: Path to the manifest JSON template file
        uuid: Unique identifier for ${UUID} substitution
        batch_id: Batch identifier for ${BATCH_ID} substitution
        data_key: Full S3 object key for the data file (replaces entire files[0].path)

    Returns:
        Manifest dict with all placeholders substituted
    """
    template_text = template_path.read_text()

    # Substitute placeholders
    substituted = template_text.replace("${UUID}", uuid).replace(
        "${BATCH_ID}", batch_id
    )

    manifest = json.loads(substituted)

    # Override the file path with the actual test data key
    manifest["files"][0]["path"] = f"s3://landing-zone/{data_key}"

    return manifest


def _upload_bytes(
    minio_client: Minio,
    bucket: str,
    object_key: str,
    data_bytes: bytes,
    content_type: str,
) -> None:
    """Upload bytes to MinIO."""
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
    """Upload manifest JSON to MinIO."""
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
    _upload_bytes(
        minio_client, bucket, manifest_key, manifest_bytes, "application/json"
    )


def _evaluate_sensor(
    sensor_fn, dagster_instance, minio_resource, expected_manifest_key: str
):
    """Evaluate sensor and return RunRequest for specific manifest."""
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
    dagster_client, job_name: str, run_request: RunRequest
) -> str:
    """Launch a job using RunRequest's config and partition."""
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

    launch_result = result["data"]["launchRun"]
    if launch_result["__typename"] != "LaunchRunSuccess":
        raise RuntimeError(f"Launch failed: {launch_result}")

    return launch_result["run"]["runId"]


def _poll_run_to_completion(
    dagster_client, run_id: str, max_wait: int = 900, poll_interval: int = 3
) -> tuple[str, Optional[dict]]:
    """Poll until run completes. Returns (status, error_details)."""
    query = """
    query RunStatus($runId: ID!) {
        runOrError(runId: $runId) {
            __typename
            ... on Run {
                status
            }
            ... on PythonError {
                message
            }
        }
    }
    """

    terminal_statuses = {"SUCCESS", "FAILURE", "CANCELED"}
    deadline = time.time() + max_wait

    while time.time() < deadline:
        result = dagster_client.query(query, {"runId": run_id})

        if "errors" in result:
            raise RuntimeError(f"GraphQL errors: {result['errors']}")

        run_data = result["data"]["runOrError"]
        if run_data["__typename"] == "Run":
            status = run_data["status"]
            if status in terminal_statuses:
                if status != "SUCCESS":
                    error_details = _get_run_error_details(dagster_client, run_id)
                    return status, error_details
                return status, None
        else:
            raise RuntimeError(f"Run error: {run_data}")

        time.sleep(poll_interval)

    raise TimeoutError(f"Run {run_id} did not complete within {max_wait}s")


def _get_run_error_details(dagster_client, run_id: str) -> Optional[dict]:
    """Fetch detailed error information from Dagster GraphQL for a failed run."""
    log_query = """
    query GetRunLogs($runId: ID!) {
        logsForRun(runId: $runId) {
            __typename
            ... on EventConnection {
                events {
                    __typename
                    ... on MessageEvent { message level }
                    ... on ExecutionStepFailureEvent {
                        stepKey
                        error {
                            message
                            stack
                            errorChain {
                                error { message stack }
                                isExplicitLink
                            }
                            cause { message stack }
                        }
                    }
                    ... on EngineEvent { message level }
                    ... on RunFailureEvent { message }
                }
            }
        }
    }
    """
    log_result = dagster_client.query(
        log_query, variables={"runId": run_id}, timeout=30
    )
    logs = log_result.get("data", {}).get("logsForRun")
    if not logs or logs.get("__typename") != "EventConnection":
        return {"error": "Could not fetch logs", "raw": log_result}

    events_list = logs.get("events", [])
    failure_events = [
        e
        for e in events_list
        if "Failure" in e.get("__typename", "") or e.get("level") == "ERROR"
    ]
    context_events = events_list[-20:] if len(events_list) > 20 else events_list

    return {
        "failure_events": failure_events,
        "context_events": context_events,
        "total_events": len(events_list),
    }


def _format_error_details(error_details: Optional[dict]) -> str:
    """Format error details into a readable string for pytest failure output."""
    if not error_details:
        return "No error details available"

    lines = ["\n" + "=" * 80, "DAGSTER RUN ERROR DETAILS", "=" * 80]

    for i, event in enumerate(error_details.get("failure_events", []), 1):
        event_type = event.get("__typename", "Unknown")
        lines.append(f"\n--- Failure Event {i}: {event_type} ---")

        if event_type == "ExecutionStepFailureEvent":
            lines.append(f"Step: {event.get('stepKey', 'unknown')}")
            error = event.get("error", {})
            if error:
                lines.append(f"\nError Message:\n{error.get('message', 'N/A')}")
                stack = error.get("stack", [])
                if stack:
                    lines.append("\nStack Trace:")
                    for frame in stack[-10:]:
                        lines.append(f"  {frame.strip()}")
                for j, chain_item in enumerate(error.get("errorChain", []), 1):
                    chain_error = chain_item.get("error", {})
                    lines.append(f"\n--- Caused By ({j}) ---")
                    lines.append(f"Message: {chain_error.get('message', 'N/A')}")
                cause = error.get("cause", {})
                if cause and cause.get("message"):
                    lines.append("\n--- Root Cause ---")
                    lines.append(f"Message: {cause.get('message', 'N/A')}")
        else:
            if event.get("message"):
                lines.append(f"Message: {event.get('message')}")

    lines.append("\n" + "=" * 80)
    return "\n".join(lines)


def _assert_mongodb_asset_exists(
    mongo_client: MongoClient, mongo_settings: MongoSettings, run_id: str
) -> dict:
    """Assert asset record exists in MongoDB and return it."""
    db = mongo_client[mongo_settings.database]
    collection = db["assets"]
    asset_doc = collection.find_one({"dagster_run_id": run_id})
    assert asset_doc is not None, f"No asset found for dagster_run_id={run_id}"
    return asset_doc


def _assert_datalake_object_exists(
    minio_client: Minio, bucket: str, s3_key: str
) -> None:
    """Assert data lake object exists."""
    try:
        minio_client.stat_object(bucket, s3_key)
    except Exception as e:
        raise AssertionError(f"Data lake object {bucket}/{s3_key} not found: {e}")


def _cleanup(
    minio_client: Minio,
    minio_settings: MinIOSettings,
    mongo_client: MongoClient,
    mongo_settings: MongoSettings,
    dagster_instance: DagsterInstance,
    landing_key: str,
    manifest_key: str,
    partition_key: str,
    asset_doc: Optional[dict],
) -> None:
    """Cleanup test artifacts."""
    # Remove landing zone data
    try:
        minio_client.remove_object(minio_settings.landing_bucket, landing_key)
    except Exception:
        pass

    # Remove manifest
    try:
        minio_client.remove_object(minio_settings.landing_bucket, manifest_key)
    except Exception:
        pass

    # Remove archived manifest
    try:
        minio_client.remove_object(
            minio_settings.landing_bucket, f"archive/{manifest_key}"
        )
    except Exception:
        pass

    # Remove datalake artifact
    if asset_doc and asset_doc.get("s3_key"):
        try:
            minio_client.remove_object(minio_settings.lake_bucket, asset_doc["s3_key"])
        except Exception:
            pass

    # Remove MongoDB asset record
    if asset_doc:
        try:
            db = mongo_client[mongo_settings.database]
            db["assets"].delete_one({"_id": asset_doc["_id"]})
        except Exception:
            pass

    # Remove partition
    try:
        dagster_instance.delete_dynamic_partition(
            partitions_def_name=dataset_partitions.name,
            partition_key=partition_key,
        )
    except Exception:
        pass


# =============================================================================
# Test Class
# =============================================================================


class TestSensorToJobE2E:
    """Test complete sensor→job→materialization flow."""

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
        """
        Test tabular sensor detects manifest and triggers job to completion.

        Flow:
        1. Upload CSV to landing zone
        2. Upload manifest to manifests/
        3. Evaluate tabular_sensor → get RunRequest
        4. Launch tabular_asset_job via GraphQL
        5. Poll to completion
        6. Verify MongoDB asset and data lake Parquet
        """
        # Generate unique identifiers
        test_uuid = uuid4().hex[:8]
        batch_id = f"sensor_e2e_tabular_{test_uuid}"
        csv_key = f"e2e/{batch_id}/data.csv"
        manifest_key = f"manifests/{batch_id}.json"

        # Load manifest from fixture template (prevents drift from fixture documentation)
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
            # 1. Upload CSV to landing zone
            csv_bytes = _load_tabular_dataset()
            _upload_bytes(
                minio_client,
                minio_settings.landing_bucket,
                csv_key,
                csv_bytes,
                "text/csv",
            )

            # 2. Upload manifest
            _upload_manifest(
                minio_client,
                minio_settings.landing_bucket,
                manifest_key,
                manifest,
            )

            # 3. Evaluate sensor
            run_request = _evaluate_sensor(
                tabular_sensor, dagster_instance, minio_resource, manifest_key
            )
            assert run_request.partition_key == dataset_id

            # 4. Launch job
            run_id = _launch_job_from_run_request(
                dagster_client, "tabular_asset_job", run_request
            )

            # 5. Poll to completion
            status, error_details = _poll_run_to_completion(dagster_client, run_id)
            if status != "SUCCESS":
                pytest.fail(
                    f"tabular_asset_job failed: {status}.{_format_error_details(error_details)}"
                )

            # 6. Verify MongoDB asset
            asset_doc = _assert_mongodb_asset_exists(
                mongo_client, mongo_settings, run_id
            )
            assert "dataset_id" in asset_doc, "Asset should have dataset_id"

            # 7. Verify data lake Parquet
            _assert_datalake_object_exists(
                minio_client, minio_settings.lake_bucket, asset_doc["s3_key"]
            )

        finally:
            _cleanup(
                minio_client,
                minio_settings,
                mongo_client,
                mongo_settings,
                dagster_instance,
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
        """
        Test spatial sensor detects manifest and triggers job to completion.

        Flow:
        1. Upload GeoJSON to landing zone
        2. Upload manifest to manifests/
        3. Evaluate spatial_sensor → get RunRequest
        4. Launch spatial_asset_job via GraphQL
        5. Poll to completion
        6. Verify MongoDB asset and data lake GeoParquet
        """
        # Generate unique identifiers
        test_uuid = uuid4().hex[:8]
        batch_id = f"sensor_e2e_spatial_{test_uuid}"
        geojson_key = f"e2e/{batch_id}/data.geojson"
        manifest_key = f"manifests/{batch_id}.json"

        # Load manifest from fixture template (prevents drift from fixture documentation)
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
            # 1. Upload GeoJSON to landing zone
            geojson_bytes = _load_spatial_dataset()
            _upload_bytes(
                minio_client,
                minio_settings.landing_bucket,
                geojson_key,
                geojson_bytes,
                "application/geo+json",
            )

            # 2. Upload manifest
            _upload_manifest(
                minio_client,
                minio_settings.landing_bucket,
                manifest_key,
                manifest,
            )

            # 3. Evaluate sensor
            run_request = _evaluate_sensor(
                spatial_sensor, dagster_instance, minio_resource, manifest_key
            )
            assert run_request.partition_key == dataset_id

            # 4. Launch job
            run_id = _launch_job_from_run_request(
                dagster_client, "spatial_asset_job", run_request
            )

            # 5. Poll to completion
            status, error_details = _poll_run_to_completion(dagster_client, run_id)
            if status != "SUCCESS":
                pytest.fail(
                    f"spatial_asset_job failed: {status}.{_format_error_details(error_details)}"
                )

            # 6. Verify MongoDB asset
            asset_doc = _assert_mongodb_asset_exists(
                mongo_client, mongo_settings, run_id
            )
            assert "dataset_id" in asset_doc, "Asset should have dataset_id"

            # 7. Verify data lake GeoParquet
            _assert_datalake_object_exists(
                minio_client, minio_settings.lake_bucket, asset_doc["s3_key"]
            )

        finally:
            _cleanup(
                minio_client,
                minio_settings,
                mongo_client,
                mongo_settings,
                dagster_instance,
                geojson_key,
                manifest_key,
                dataset_id,
                asset_doc,
            )
