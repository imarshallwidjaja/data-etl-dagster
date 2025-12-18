"""Integration test: End-to-end tabular_asset_job pipeline via Dagster GraphQL.

This test validates the tabular offline-first ETL loop:
1. Upload a sample CSV to landing-zone
2. Launch tabular_asset_job via GraphQL with manifest input + partition key tag
3. Verify data-lake Parquet object exists
4. Verify MongoDB asset record exists and has tabular-specific fields
5. Cleanup MinIO + Mongo artifacts

Run with: pytest tests/integration/test_tabular_asset_e2e.py -v -m "integration and e2e"
"""

import json
import os
from pathlib import Path
import time
from io import BytesIO
from typing import Optional
from uuid import uuid4

import pytest
import requests
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient
from requests.exceptions import RequestException, Timeout

from libs.models import MinIOSettings, MongoSettings


pytestmark = [pytest.mark.integration, pytest.mark.e2e]

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "asset_plans"
TABULAR_DATASET_PATH = FIXTURES_DIR / "e2e_sample_table_data.csv"
TABULAR_MANIFEST_TEMPLATE_PATH = FIXTURES_DIR / "e2e_tabular_manifest.json"


@pytest.fixture
def dagster_graphql_url():
    port = os.getenv("DAGSTER_WEBSERVER_PORT", "3000")
    return f"http://localhost:{port}/graphql"


@pytest.fixture
def dagster_client(dagster_graphql_url):
    class DagsterGraphQLClient:
        def __init__(self, url: str):
            self.url = url

        def query(
            self, query_str: str, variables: Optional[dict] = None, timeout: int = 30
        ) -> dict:
            payload = {"query": query_str}
            if variables:
                payload["variables"] = variables

            try:
                response = requests.post(self.url, json=payload, timeout=timeout)
                if response.status_code != 200:
                    raise RuntimeError(
                        f"GraphQL request failed with status {response.status_code}: "
                        f"{response.text}"
                    )
                return response.json()
            except (RequestException, Timeout) as e:
                raise RuntimeError(
                    f"Failed to communicate with Dagster GraphQL API: {e}"
                ) from e

    return DagsterGraphQLClient(dagster_graphql_url)


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
def mongo_settings():
    return MongoSettings()


@pytest.fixture
def mongo_client(mongo_settings):
    return MongoClient(
        mongo_settings.connection_string,
        serverSelectionTimeoutMS=5000,
    )


def _create_dummy_csv() -> bytes:
    return TABULAR_DATASET_PATH.read_bytes()


def _load_manifest_template() -> dict:
    return json.loads(TABULAR_MANIFEST_TEMPLATE_PATH.read_text())


def _add_dynamic_partition(dagster_client, partition_key: str) -> None:
    """Register a dynamic partition key via GraphQL before launching a partitioned job.

    Required when bypassing the sensor path which normally handles partition registration.
    """
    mutation = """
    mutation AddDynamicPartition(
        $repositoryLocationName: String!
        $repositoryName: String!
        $partitionsDefName: String!
        $partitionKey: String!
    ) {
        addDynamicPartition(
            repositorySelector: {
                repositoryLocationName: $repositoryLocationName
                repositoryName: $repositoryName
            }
            partitionsDefName: $partitionsDefName
            partitionKey: $partitionKey
        ) {
            ... on AddDynamicPartitionSuccess { partitionsDefName partitionKey }
            ... on PythonError { message }
        }
    }
    """
    result = dagster_client.query(
        mutation,
        variables={
            "repositoryLocationName": "etl_pipelines",
            "repositoryName": "__repository__",
            "partitionsDefName": "dataset_id",
            "partitionKey": partition_key,
        },
        timeout=10,
    )
    assert "errors" not in result, (
        f"Failed to add dynamic partition: {result.get('errors')}"
    )


def _upload_csv_to_landing_zone(
    minio_client: Minio,
    landing_bucket: str,
    object_key: str,
    csv_bytes: bytes,
) -> None:
    data = BytesIO(csv_bytes)
    minio_client.put_object(
        landing_bucket,
        object_key,
        data,
        length=len(csv_bytes),
        content_type="text/csv",
    )


def _launch_tabular_asset_job(dagster_client, manifest: dict) -> str:
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
            ... on LaunchRunSuccess {
                run { runId status }
            }
            ... on PipelineNotFoundError { message }
            ... on RunConfigValidationInvalid { errors { message } }
            ... on PythonError { message }
        }
    }
    """

    variables = {
        "repositoryLocationName": "etl_pipelines",
        "repositoryName": "__repository__",
        "jobName": "tabular_asset_job",
        "runConfigData": {
            "ops": {"raw_manifest_json": {"config": {"manifest": manifest}}}
        },
        "executionMetadata": {
            "tags": [
                {
                    "key": "dagster/partition",
                    "value": manifest["metadata"]["tags"]["dataset_id"],
                },
            ]
        },
    }

    result = dagster_client.query(launch_query, variables=variables, timeout=10)
    assert "errors" not in result, f"Failed to launch job: {result.get('errors')}"

    launch_response = result["data"]["launchRun"]
    assert "run" in launch_response, (
        f"Job launch failed: {launch_response.get('message', 'Unknown error')}"
    )

    run_id = launch_response["run"]["runId"]
    assert run_id, "No run_id returned from job launch"
    return run_id


def _poll_run_to_completion(
    dagster_client,
    run_id: str,
    max_wait: int = 600,
    poll_interval: int = 3,
) -> tuple[str, Optional[dict]]:
    elapsed = 0
    status = "STARTING"
    error_details = None

    while elapsed < max_wait:
        run_query = """
        query GetRun($runId: ID!) {
            runOrError(runId: $runId) {
                ... on Run { id status }
                ... on RunNotFoundError { message }
            }
        }
        """

        run_result = dagster_client.query(
            run_query, variables={"runId": run_id}, timeout=10
        )
        assert "errors" not in run_result, (
            f"Failed to query run status: {run_result.get('errors')}"
        )

        run_or_error = run_result["data"]["runOrError"]
        if "id" not in run_or_error:
            time.sleep(poll_interval)
            elapsed += poll_interval
            continue

        status = run_or_error["status"]
        if status in ["SUCCESS", "FAILURE", "CANCELED"]:
            break

        time.sleep(poll_interval)
        elapsed += poll_interval

    if status != "SUCCESS":
        error_details = _get_run_error_details(dagster_client, run_id)

    return status, error_details


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
    mongo_client: MongoClient,
    mongo_settings: MongoSettings,
    run_id: str,
) -> dict:
    db = mongo_client[mongo_settings.database]
    collection = db["assets"]

    asset_doc = collection.find_one({"dagster_run_id": run_id})
    assert asset_doc is not None, (
        f"No asset record found in MongoDB for run_id: {run_id}"
    )
    return asset_doc


def _assert_datalake_object_exists(
    minio_client: Minio,
    lake_bucket: str,
    s3_key: str,
) -> None:
    try:
        stat = minio_client.stat_object(lake_bucket, s3_key)
        assert stat.size > 0, f"Data-lake object {s3_key} has zero size"
    except S3Error as e:
        pytest.fail(f"Data-lake object {s3_key} does not exist: {e}")


def _cleanup_landing_zone_object(
    minio_client: Minio,
    landing_bucket: str,
    object_key: str,
) -> None:
    try:
        minio_client.remove_object(landing_bucket, object_key)
    except S3Error:
        pass


def _cleanup_asset_artifacts(
    minio_client: Minio,
    mongo_client: MongoClient,
    mongo_settings: MongoSettings,
    lake_bucket: str,
    run_id: str,
    asset_doc: Optional[dict],
) -> None:
    if asset_doc is None:
        return

    s3_key = asset_doc.get("s3_key")
    if s3_key:
        try:
            minio_client.remove_object(lake_bucket, s3_key)
        except S3Error:
            pass

    db = mongo_client[mongo_settings.database]
    collection = db["assets"]
    try:
        collection.delete_many({"dagster_run_id": run_id})
    except Exception:
        pass


class TestTabularAssetJobE2E:
    def test_tabular_asset_job_full_pipeline(
        self,
        dagster_client,
        minio_client,
        minio_settings,
        mongo_client,
        mongo_settings,
    ):
        batch_id = f"e2e_tabular_{uuid4().hex[:12]}"
        object_key = f"e2e/{batch_id}/data.csv"

        csv_bytes = _create_dummy_csv()
        # Load template and customize dynamic values
        manifest = _load_manifest_template()
        manifest["batch_id"] = batch_id
        manifest["files"][0]["path"] = f"s3://landing-zone/{object_key}"
        manifest["metadata"]["tags"] = dict(manifest["metadata"].get("tags", {}))
        manifest["metadata"]["tags"]["dataset_id"] = f"dataset_{batch_id}"
        manifest["metadata"]["tags"]["priority"] = 1

        run_id: str | None = None
        asset_doc: dict | None = None

        try:
            _upload_csv_to_landing_zone(
                minio_client,
                minio_settings.landing_bucket,
                object_key,
                csv_bytes,
            )

            # Register partition before launching (bypasses sensor path)
            _add_dynamic_partition(
                dagster_client, manifest["metadata"]["tags"]["dataset_id"]
            )

            run_id = _launch_tabular_asset_job(dagster_client, manifest)

            status, error_details = _poll_run_to_completion(dagster_client, run_id)
            if status != "SUCCESS":
                pytest.fail(
                    f"tabular_asset_job failed: {status}.{_format_error_details(error_details)}"
                )

            asset_doc = _assert_mongodb_asset_exists(
                mongo_client,
                mongo_settings,
                run_id,
            )

            assert asset_doc.get("kind") == "tabular"
            assert asset_doc.get("format") == "parquet"
            assert "crs" not in asset_doc or asset_doc.get("crs") is None
            assert "bounds" not in asset_doc or asset_doc.get("bounds") is None

            metadata = asset_doc.get("metadata") or {}
            tags = metadata.get("tags") or {}
            assert tags.get("priority") == 1

            header_mapping = metadata.get("header_mapping")
            assert isinstance(header_mapping, dict)
            # Fixture CSV (`e2e_sample_table_data.csv`) already uses snake_case headers.
            assert header_mapping.get("ogc_fid") == "ogc_fid"
            assert header_mapping.get("sa1_code21") == "sa1_code21"

            s3_key = asset_doc.get("s3_key")
            assert s3_key, "Asset document missing s3_key"
            _assert_datalake_object_exists(
                minio_client,
                minio_settings.lake_bucket,
                s3_key,
            )

        finally:
            _cleanup_landing_zone_object(
                minio_client,
                minio_settings.landing_bucket,
                object_key,
            )
            if run_id is not None:
                _cleanup_asset_artifacts(
                    minio_client,
                    mongo_client,
                    mongo_settings,
                    minio_settings.lake_bucket,
                    run_id,
                    asset_doc,
                )

    def test_tabular_asset_job_with_join_key_normalization(
        self,
        dagster_client,
        minio_client,
        minio_settings,
        mongo_client,
        mongo_settings,
    ):
        batch_id = f"e2e_tabular_join_{uuid4().hex[:12]}"
        object_key = f"e2e/{batch_id}/data.csv"

        csv_bytes = _create_dummy_csv()
        # Load template and customize for join-key normalization test
        manifest = _load_manifest_template()
        manifest["batch_id"] = batch_id
        manifest["files"][0]["path"] = f"s3://landing-zone/{object_key}"
        manifest["metadata"]["tags"] = dict(manifest["metadata"].get("tags", {}))
        manifest["metadata"]["tags"]["dataset_id"] = f"dataset_{batch_id}"
        manifest["metadata"]["tags"]["priority"] = 1
        # Add join_config with placeholder asset IDs for join key normalization test
        # Note: For ingest_tabular, join_config is optional but triggers join key normalization
        manifest["metadata"]["join_config"] = {
            "spatial_asset_id": "000000000000000000000000",  # Placeholder ObjectId
            "tabular_asset_id": "000000000000000000000001",  # Placeholder ObjectId
            # Use a real column from the fixture CSV.
            "left_key": "sa1_code21",
            "right_key": "sa1_code21",
            "how": "left",
        }

        run_id: str | None = None
        asset_doc: dict | None = None

        try:
            _upload_csv_to_landing_zone(
                minio_client,
                minio_settings.landing_bucket,
                object_key,
                csv_bytes,
            )

            # Register partition before launching (bypasses sensor path)
            _add_dynamic_partition(
                dagster_client, manifest["metadata"]["tags"]["dataset_id"]
            )

            run_id = _launch_tabular_asset_job(dagster_client, manifest)

            status, error_details = _poll_run_to_completion(dagster_client, run_id)
            if status != "SUCCESS":
                pytest.fail(
                    f"tabular_asset_job failed: {status}.{_format_error_details(error_details)}"
                )

            asset_doc = _assert_mongodb_asset_exists(
                mongo_client,
                mongo_settings,
                run_id,
            )
            metadata = asset_doc.get("metadata") or {}
            tags = metadata.get("tags") or {}
            assert tags.get("join_key_clean") == "sa1_code21"

        finally:
            _cleanup_landing_zone_object(
                minio_client, minio_settings.landing_bucket, object_key
            )
            if run_id is not None:
                _cleanup_asset_artifacts(
                    minio_client,
                    mongo_client,
                    mongo_settings,
                    minio_settings.lake_bucket,
                    run_id,
                    asset_doc,
                )
