"""Integration test: End-to-end spatial_asset_job pipeline via Dagster GraphQL.

This test validates the spatial asset materialization flow:
1. Upload sample GeoJSON to landing-zone
2. Launch spatial_asset_job via GraphQL with manifest + partition key
3. Verify MongoDB asset record exists (kind="spatial")
4. Verify data-lake GeoParquet exists
5. Verify PostGIS ephemeral schema is cleaned up

Run with: pytest tests/integration/test_spatial_asset_e2e.py -v -m "integration and e2e"
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
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient
from requests.exceptions import RequestException, Timeout

from libs.models import MinIOSettings, MongoSettings, PostGISSettings
from libs.spatial_utils import RunIdSchemaMapping


pytestmark = [pytest.mark.integration, pytest.mark.e2e]

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "asset_plans"
MANIFEST_TEMPLATE_PATH = FIXTURES_DIR / "e2e_spatial_manifest.json"
DATASET_PATH = FIXTURES_DIR / "e2e_sample_sa1_data.json"


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
    return json.loads(MANIFEST_TEMPLATE_PATH.read_text())


def _load_fixture_dataset() -> bytes:
    return DATASET_PATH.read_bytes()


def _upload_bytes(
    minio_client: Minio,
    bucket: str,
    object_key: str,
    data_bytes: bytes,
    *,
    content_type: str,
) -> None:
    minio_client.put_object(
        bucket,
        object_key,
        BytesIO(data_bytes),
        length=len(data_bytes),
        content_type=content_type,
    )


def _add_dynamic_partition(dagster_client, partition_key: str) -> None:
    """Register a dynamic partition key via GraphQL before launching a partitioned job.

    Required when bypassing the sensor path which normally handles partition registration.
    """
    mutation = """
    mutation AddDynamicPartition($partitionsDefName: String!, $partitionKey: String!) {
        addDynamicPartition(partitionsDefName: $partitionsDefName, partitionKey: $partitionKey) {
            ... on AddDynamicPartitionSuccess { partitionsDefName partitionKey }
            ... on PythonError { message }
        }
    }
    """
    result = dagster_client.query(
        mutation,
        variables={"partitionsDefName": "dataset_id", "partitionKey": partition_key},
        timeout=10,
    )
    assert "errors" not in result, (
        f"Failed to add dynamic partition: {result.get('errors')}"
    )


def _launch_spatial_asset_job(
    dagster_client, *, manifest: dict, partition_key: str
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

    variables = {
        "repositoryLocationName": "etl_pipelines",
        "repositoryName": "__repository__",
        "jobName": "spatial_asset_job",
        "runConfigData": {
            "ops": {"raw_manifest_json": {"config": {"manifest": manifest}}}
        },
        "executionMetadata": {
            "tags": [{"key": "dagster/partition", "value": partition_key}]
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
    log_query = """
    query GetRunLogs($runId: ID!) {
        logsForRun(runId: $runId) {
            __typename
            ... on EventConnection {
                events {
                    __typename
                    ... on MessageEvent { message level }
                    ... on ExecutionStepFailureEvent { error { message stack } }
                }
            }
        }
    }
    """
    log_result = dagster_client.query(
        log_query, variables={"runId": run_id}, timeout=10
    )
    logs = log_result.get("data", {}).get("logsForRun")
    if not logs or logs.get("__typename") != "EventConnection":
        return None
    events_list = logs.get("events", [])
    error_events = [
        e
        for e in events_list
        if e.get("level") == "ERROR" or "Failure" in e.get("__typename", "")
    ]
    return {"events": error_events or events_list[-10:]}


def _assert_mongodb_asset_exists(
    mongo_client: MongoClient, mongo_settings: MongoSettings, run_id: str
) -> dict:
    db = mongo_client[mongo_settings.database]
    asset_doc = db["assets"].find_one({"dagster_run_id": run_id})
    assert asset_doc is not None, f"No asset record found for run_id: {run_id}"
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
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
            (schema_name,),
        )
        result = cur.fetchone()
        assert result is None, f"PostGIS schema {schema_name} still exists"


def _cleanup(
    minio_client: Minio,
    minio_settings: MinIOSettings,
    mongo_client: MongoClient,
    mongo_settings: MongoSettings,
    landing_key: str,
    asset_doc: Optional[dict],
) -> None:
    try:
        minio_client.remove_object(minio_settings.landing_bucket, landing_key)
    except Exception:
        pass

    if asset_doc:
        try:
            minio_client.remove_object(minio_settings.lake_bucket, asset_doc["s3_key"])
        except Exception:
            pass
        try:
            mongo_client[mongo_settings.database]["assets"].delete_one(
                {"_id": asset_doc["_id"]}
            )
        except Exception:
            pass


class TestSpatialAssetE2E:
    def test_spatial_asset_job_full_pipeline(
        self,
        dagster_client,
        minio_client,
        minio_settings,
        mongo_client,
        mongo_settings,
        postgis_connection,
    ):
        batch_id = f"e2e_spatial_asset_{uuid4().hex[:12]}"
        partition_key = f"spatial_{batch_id}"
        landing_key = f"e2e/{batch_id}/e2e_sample_sa1_data.json"

        manifest_template = _load_fixture_manifest()
        dataset_bytes = _load_fixture_dataset()

        manifest = manifest_template.copy()
        manifest["batch_id"] = batch_id
        manifest["files"][0]["path"] = f"s3://landing-zone/{landing_key}"
        manifest["files"][0]["type"] = "vector"

        # Normalize tags (fixture stores some values as strings; pipeline expects primitives, but strings are OK)
        manifest["metadata"]["tags"] = dict(
            manifest.get("metadata", {}).get("tags", {})
        )
        manifest["metadata"]["tags"]["dataset_id"] = partition_key

        run_id: str | None = None
        asset_doc: dict | None = None

        try:
            _upload_bytes(
                minio_client,
                minio_settings.landing_bucket,
                landing_key,
                dataset_bytes,
                content_type="application/json",
            )

            # Register partition before launching (bypasses sensor path)
            _add_dynamic_partition(dagster_client, partition_key)

            run_id = _launch_spatial_asset_job(
                dagster_client, manifest=manifest, partition_key=partition_key
            )

            status, error_details = _poll_run_to_completion(dagster_client, run_id)
            if status != "SUCCESS":
                pytest.fail(
                    f"spatial_asset_job failed: {status}. Details: {error_details}"
                )

            asset_doc = _assert_mongodb_asset_exists(
                mongo_client, mongo_settings, run_id
            )
            assert asset_doc.get("kind") == "spatial", (
                f"Expected kind=spatial, got {asset_doc.get('kind')}"
            )

            _assert_datalake_object_exists(
                minio_client, minio_settings.lake_bucket, asset_doc["s3_key"]
            )
            _assert_postgis_schema_cleaned(postgis_connection, run_id)

        finally:
            _cleanup(
                minio_client,
                minio_settings,
                mongo_client,
                mongo_settings,
                landing_key,
                asset_doc,
            )
