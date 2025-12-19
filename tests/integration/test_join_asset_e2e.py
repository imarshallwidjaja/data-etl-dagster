"""Integration test: End-to-end join_asset_job pipeline via Dagster GraphQL.

This test validates the join offline-first ETL loop:
1. Ingest a spatial dataset via `spatial_asset_job` (creates spatial asset in Mongo + GeoParquet in data-lake)
2. Upload a tabular CSV to landing-zone with a join key matching the spatial dataset
3. Launch `join_asset_job` via GraphQL with `raw_manifest_json` asset config
4. Verify joined GeoParquet exists in data-lake
5. Verify MongoDB joined asset record exists
6. Verify MongoDB lineage edge exists (spatial parent -> joined)
7. Verify PostGIS ephemeral schema is cleaned up
"""

from __future__ import annotations

import json
import os
import time
from io import BytesIO
from pathlib import Path
from typing import Optional
from uuid import uuid4

import psycopg2
import pytest
import requests
from bson import ObjectId
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient
from requests.exceptions import RequestException, Timeout

from libs.models import MinIOSettings, MongoSettings, PostGISSettings
from libs.spatial_utils import RunIdSchemaMapping


pytestmark = [pytest.mark.integration, pytest.mark.e2e]

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "asset_plans"
SPATIAL_MANIFEST_TEMPLATE_PATH = FIXTURES_DIR / "e2e_spatial_manifest.json"
SPATIAL_DATASET_PATH = FIXTURES_DIR / "e2e_sample_sa1_data.json"
TABULAR_DATASET_PATH = FIXTURES_DIR / "e2e_sample_table_data.csv"
JOIN_MANIFEST_TEMPLATE_PATH = FIXTURES_DIR / "e2e_join_manifest.json"


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
    return MongoClient(mongo_settings.connection_string, serverSelectionTimeoutMS=5000)


@pytest.fixture
def postgis_settings():
    return PostGISSettings()


@pytest.fixture
def postgis_connection(postgis_settings):
    conn = psycopg2.connect(postgis_settings.connection_string, connect_timeout=5)
    yield conn
    conn.close()


def _load_fixture_manifest() -> dict:
    return json.loads(SPATIAL_MANIFEST_TEMPLATE_PATH.read_text())


def _load_fixture_dataset() -> bytes:
    return SPATIAL_DATASET_PATH.read_bytes()


def _load_tabular_fixture_dataset() -> bytes:
    return TABULAR_DATASET_PATH.read_bytes()


def _load_join_manifest_template() -> dict:
    return json.loads(JOIN_MANIFEST_TEMPLATE_PATH.read_text())


def _upload_bytes(
    minio_client: Minio,
    bucket: str,
    object_key: str,
    data_bytes: bytes,
    *,
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


def _launch_job_with_run_config(
    dagster_client, *, job_name: str, run_config: dict, partition_key: str | None = None
) -> str:
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


def _poll_run_to_completion(
    dagster_client,
    run_id: str,
    *,
    max_wait: int = 900,
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
    """Fetch detailed error information from Dagster GraphQL for a failed run.

    Captures the full error chain including cause and nested errors for better
    CI/CD debugging visibility.
    """
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
                                error {
                                    message
                                    stack
                                }
                                isExplicitLink
                            }
                            cause {
                                message
                                stack
                            }
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

    # Collect all failure events with full error details
    failure_events = []
    for e in events_list:
        if "Failure" in e.get("__typename", ""):
            failure_events.append(e)
        elif e.get("level") == "ERROR":
            failure_events.append(e)

    # Also get last N events for context
    context_events = events_list[-20:] if len(events_list) > 20 else events_list

    return {
        "failure_events": failure_events,
        "context_events": context_events,
        "total_events": len(events_list),
    }


def _format_error_details(error_details: Optional[dict]) -> str:
    """Format error details into a readable string for pytest failure output.

    Extracts the most important error information and formats it for
    easy reading in CI/CD logs.
    """
    if not error_details:
        return "No error details available"

    lines = []
    lines.append("\n" + "=" * 80)
    lines.append("DAGSTER RUN ERROR DETAILS")
    lines.append("=" * 80)

    failure_events = error_details.get("failure_events", [])

    for i, event in enumerate(failure_events, 1):
        event_type = event.get("__typename", "Unknown")
        lines.append(f"\n--- Failure Event {i}: {event_type} ---")

        if event_type == "ExecutionStepFailureEvent":
            step_key = event.get("stepKey", "unknown")
            lines.append(f"Step: {step_key}")

            error = event.get("error", {})
            if error:
                lines.append(f"\nError Message:\n{error.get('message', 'N/A')}")

                # Print stack trace
                stack = error.get("stack", [])
                if stack:
                    lines.append("\nStack Trace:")
                    for frame in stack[-10:]:  # Last 10 frames
                        lines.append(f"  {frame.strip()}")

                # Print error chain (nested causes)
                error_chain = error.get("errorChain", [])
                for j, chain_item in enumerate(error_chain, 1):
                    chain_error = chain_item.get("error", {})
                    lines.append(f"\n--- Caused By ({j}) ---")
                    lines.append(f"Message: {chain_error.get('message', 'N/A')}")
                    chain_stack = chain_error.get("stack", [])
                    if chain_stack:
                        lines.append("Stack:")
                        for frame in chain_stack[-5:]:
                            lines.append(f"  {frame.strip()}")

                # Print direct cause
                cause = error.get("cause", {})
                if cause and cause.get("message"):
                    lines.append("\n--- Root Cause ---")
                    lines.append(f"Message: {cause.get('message', 'N/A')}")
                    cause_stack = cause.get("stack", [])
                    if cause_stack:
                        lines.append("Stack:")
                        for frame in cause_stack[-5:]:
                            lines.append(f"  {frame.strip()}")
        else:
            # Generic error event
            message = event.get("message", "")
            if message:
                lines.append(f"Message: {message}")

    lines.append("\n" + "=" * 80)
    return "\n".join(lines)


def _assert_mongodb_asset_exists(
    mongo_client: MongoClient, mongo_settings: MongoSettings, run_id: str
) -> dict:
    db = mongo_client[mongo_settings.database]
    asset_doc = db["assets"].find_one({"dagster_run_id": run_id})
    assert asset_doc is not None, (
        f"No asset record found in MongoDB for run_id: {run_id}"
    )
    return asset_doc


def _assert_datalake_object_exists(
    minio_client: Minio, bucket: str, s3_key: str
) -> None:
    try:
        stat = minio_client.stat_object(bucket, s3_key)
        assert stat.size > 0, f"Data-lake object {s3_key} has zero size"
    except S3Error as e:
        pytest.fail(f"Data-lake object {s3_key} does not exist: {e}")


def _assert_postgis_schema_cleaned(postgis_connection, run_id: str) -> None:
    schema_mapping = RunIdSchemaMapping.from_run_id(run_id)
    schema_name = schema_mapping.schema_name

    with postgis_connection.cursor() as cur:
        cur.execute(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = %s
            """,
            (schema_name,),
        )
        result = cur.fetchone()
        assert result is None, (
            f"PostGIS schema {schema_name} still exists after job completion (should be cleaned up)"
        )


def _extract_join_key_and_value(spatial_dataset_bytes: bytes) -> tuple[str, str]:
    data = json.loads(spatial_dataset_bytes.decode("utf-8"))
    feature0 = data["features"][0]
    props = feature0.get("properties", {})

    key = "sa1_code" if "sa1_code" in props else None
    if key is None:
        candidates = [
            k for k in props.keys() if "sa1" in k.lower() and "co" in k.lower()
        ]
        assert candidates, "Could not find a join key candidate in fixture properties"
        key = candidates[0]

    value = props.get(key)
    assert value is not None, f"Join key value is None for key={key}"
    return key, str(value)


def _cleanup_landing_zone_object(
    minio_client: Minio, landing_bucket: str, object_key: str
) -> None:
    try:
        minio_client.remove_object(landing_bucket, object_key)
    except S3Error:
        pass


class TestJoinAssetE2E:
    def test_join_asset_job_full_pipeline(
        self,
        dagster_client,
        minio_client,
        minio_settings,
        mongo_client,
        mongo_settings,
        postgis_connection,
    ):
        """Test full join_asset_job pipeline with asset-only inputs.

        Flow:
        1. Create spatial parent via spatial_asset_job
        2. Create tabular parent via tabular_asset_job
        3. Launch join_asset_job with both asset IDs
        4. Verify joined asset in MongoDB + data-lake
        5. Verify lineage edges for both parents
        6. Verify PostGIS cleanup
        """
        batch_id = f"e2e_join_{uuid4().hex[:12]}"
        spatial_partition_key = f"spatial_{batch_id}"
        tabular_partition_key = f"tabular_{batch_id}"
        join_partition_key = f"joined_{batch_id}"

        spatial_object_key = f"e2e/{batch_id}/spatial.json"
        tabular_object_key = f"e2e/{batch_id}/tabular.csv"

        spatial_run_id: str | None = None
        tabular_run_id: str | None = None
        join_run_id: str | None = None
        spatial_asset_doc: dict | None = None
        tabular_asset_doc: dict | None = None
        joined_asset_doc: dict | None = None

        try:
            # =========================================================
            # 1) Create spatial parent via spatial_asset_job
            # =========================================================
            spatial_manifest_template = _load_fixture_manifest()
            spatial_dataset_bytes = _load_fixture_dataset()

            spatial_manifest = spatial_manifest_template.copy()
            spatial_manifest["batch_id"] = batch_id
            spatial_manifest["intent"] = "ingest_vector"
            spatial_manifest["files"][0]["path"] = (
                f"s3://landing-zone/{spatial_object_key}"
            )
            spatial_manifest["files"][0]["type"] = "vector"
            spatial_manifest["metadata"]["tags"] = dict(
                spatial_manifest.get("metadata", {}).get("tags", {})
            )
            spatial_manifest["metadata"]["tags"]["dataset_id"] = spatial_partition_key

            _upload_bytes(
                minio_client,
                minio_settings.landing_bucket,
                spatial_object_key,
                spatial_dataset_bytes,
                content_type="application/json",
            )

            # Register partition before launching (bypasses sensor path)
            _add_dynamic_partition(dagster_client, spatial_partition_key)

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
            status, error_details = _poll_run_to_completion(
                dagster_client, spatial_run_id
            )
            if status != "SUCCESS":
                pytest.fail(
                    f"spatial_asset_job failed: {status}.{_format_error_details(error_details)}"
                )

            spatial_asset_doc = _assert_mongodb_asset_exists(
                mongo_client, mongo_settings, spatial_run_id
            )
            spatial_dataset_id = spatial_asset_doc["dataset_id"]
            spatial_object_id = str(spatial_asset_doc["_id"])  # For lineage assertion

            # =========================================================
            # 2) Create tabular parent via tabular_asset_job
            # =========================================================
            tabular_dataset_bytes = _load_tabular_fixture_dataset()

            tabular_manifest = {
                "batch_id": f"tabular_{batch_id}",
                "uploader": "e2e_test",
                "intent": "ingest_tabular",
                "files": [
                    {
                        "path": f"s3://landing-zone/{tabular_object_key}",
                        "type": "tabular",
                        "format": "CSV",
                    }
                ],
                "metadata": {
                    "project": "E2E_JOIN_TEST",
                    "description": "Tabular parent for join test",
                    "tags": {"dataset_id": tabular_partition_key},
                },
            }

            _upload_bytes(
                minio_client,
                minio_settings.landing_bucket,
                tabular_object_key,
                tabular_dataset_bytes,
                content_type="text/csv",
            )

            # Register partition before launching (bypasses sensor path)
            _add_dynamic_partition(dagster_client, tabular_partition_key)

            tabular_run_id = _launch_job_with_run_config(
                dagster_client,
                job_name="tabular_asset_job",
                run_config={
                    "ops": {
                        "raw_manifest_json": {"config": {"manifest": tabular_manifest}}
                    }
                },
                partition_key=tabular_partition_key,
            )
            status, error_details = _poll_run_to_completion(
                dagster_client, tabular_run_id
            )
            if status != "SUCCESS":
                pytest.fail(
                    f"tabular_asset_job failed: {status}.{_format_error_details(error_details)}"
                )

            tabular_asset_doc = _assert_mongodb_asset_exists(
                mongo_client, mongo_settings, tabular_run_id
            )
            tabular_dataset_id = tabular_asset_doc["dataset_id"]
            tabular_object_id = str(tabular_asset_doc["_id"])  # For lineage assertion

            # =========================================================
            # 3) Launch join_asset_job with both asset IDs (no files)
            # =========================================================
            join_manifest = _load_join_manifest_template()
            join_manifest = join_manifest.copy()
            join_manifest["batch_id"] = f"join_{batch_id}"
            join_manifest["metadata"]["tags"] = dict(
                join_manifest["metadata"].get("tags", {})
            )
            join_manifest["metadata"]["tags"]["dataset_id"] = join_partition_key
            # Use dataset_id instead of MongoDB ObjectId for join config
            join_manifest["metadata"]["join_config"]["spatial_dataset_id"] = (
                spatial_dataset_id
            )
            join_manifest["metadata"]["join_config"]["tabular_dataset_id"] = (
                tabular_dataset_id
            )

            # Register partition before launching (bypasses sensor path)
            _add_dynamic_partition(dagster_client, join_partition_key)

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
            status, error_details = _poll_run_to_completion(dagster_client, join_run_id)
            if status != "SUCCESS":
                pytest.fail(
                    f"join_asset_job failed: {status}.{_format_error_details(error_details)}"
                )

            # =========================================================
            # 4) Verify joined asset in MongoDB + data-lake
            # =========================================================
            joined_asset_doc = _assert_mongodb_asset_exists(
                mongo_client, mongo_settings, join_run_id
            )
            assert joined_asset_doc.get("kind") == "joined"
            _assert_datalake_object_exists(
                minio_client, minio_settings.lake_bucket, joined_asset_doc["s3_key"]
            )

            # =========================================================
            # 5) Verify lineage edges (both parents -> joined)
            # =========================================================
            db = mongo_client[mongo_settings.database]
            joined_id = joined_asset_doc["_id"]

            # Verify lineage uses MongoDB ObjectIds (internal detail)
            spatial_lineage = db["lineage"].find_one(
                {
                    "source_asset_id": ObjectId(spatial_object_id),
                    "target_asset_id": joined_id,
                }
            )
            assert spatial_lineage is not None, "Spatial parent lineage not found"
            assert spatial_lineage["transformation"] == "spatial_join"

            tabular_lineage = db["lineage"].find_one(
                {
                    "source_asset_id": ObjectId(tabular_object_id),
                    "target_asset_id": joined_id,
                }
            )
            assert tabular_lineage is not None, "Tabular parent lineage not found"
            assert tabular_lineage["transformation"] == "spatial_join"

            # =========================================================
            # 6) Verify PostGIS cleanup
            # =========================================================
            _assert_postgis_schema_cleaned(postgis_connection, join_run_id)

        finally:
            _cleanup_landing_zone_object(
                minio_client, minio_settings.landing_bucket, spatial_object_key
            )
            _cleanup_landing_zone_object(
                minio_client, minio_settings.landing_bucket, tabular_object_key
            )

            db = mongo_client[mongo_settings.database]
            for doc in [joined_asset_doc, spatial_asset_doc, tabular_asset_doc]:
                if doc is None:
                    continue
                try:
                    minio_client.remove_object(
                        minio_settings.lake_bucket, doc["s3_key"]
                    )
                except Exception:
                    pass
                try:
                    db["assets"].delete_one({"_id": doc["_id"]})
                except Exception:
                    pass

            try:
                asset_ids = [
                    d["_id"]
                    for d in [spatial_asset_doc, tabular_asset_doc, joined_asset_doc]
                    if d is not None
                ]
                if asset_ids:
                    db["lineage"].delete_many(
                        {
                            "$or": [
                                {"source_asset_id": {"$in": asset_ids}},
                                {"target_asset_id": {"$in": asset_ids}},
                            ]
                        }
                    )
            except Exception:
                pass
