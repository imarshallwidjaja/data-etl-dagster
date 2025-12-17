"""Integration test: End-to-end join_asset_job pipeline via Dagster GraphQL.

This test validates the join offline-first ETL loop:
1. Ingest a spatial dataset via `ingest_job` (creates spatial asset in Mongo + GeoParquet in data-lake)
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

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SPATIAL_MANIFEST_TEMPLATE_PATH = FIXTURES_DIR / "e2e_sample_sa1_data-manifest.json"
SPATIAL_DATASET_PATH = FIXTURES_DIR / "e2e_sample_sa1_data.json"


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
    log_result = dagster_client.query(log_query, variables={"runId": run_id}, timeout=10)
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


def _assert_mongodb_asset_exists(mongo_client: MongoClient, mongo_settings: MongoSettings, run_id: str) -> dict:
    db = mongo_client[mongo_settings.database]
    asset_doc = db["assets"].find_one({"dagster_run_id": run_id})
    assert asset_doc is not None, f"No asset record found in MongoDB for run_id: {run_id}"
    return asset_doc


def _assert_datalake_object_exists(minio_client: Minio, bucket: str, s3_key: str) -> None:
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
        candidates = [k for k in props.keys() if "sa1" in k.lower() and "co" in k.lower()]
        assert candidates, "Could not find a join key candidate in fixture properties"
        key = candidates[0]

    value = props.get(key)
    assert value is not None, f"Join key value is None for key={key}"
    return key, str(value)


def _cleanup_landing_zone_object(minio_client: Minio, landing_bucket: str, object_key: str) -> None:
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
        batch_id = f"e2e_join_{uuid4().hex[:12]}"
        spatial_object_key = f"e2e/{batch_id}/spatial.json"
        tabular_object_key = f"e2e/{batch_id}/tabular.csv"

        ingest_run_id: str | None = None
        join_run_id: str | None = None
        spatial_asset_doc: dict | None = None
        joined_asset_doc: dict | None = None

        try:
            # 1) Ingest spatial parent via ingest_job
            spatial_manifest_template = _load_fixture_manifest()
            spatial_dataset_bytes = _load_fixture_dataset()

            spatial_manifest = spatial_manifest_template.copy()
            spatial_manifest["batch_id"] = batch_id
            spatial_manifest["files"][0]["path"] = f"s3://landing-zone/{spatial_object_key}"

            _upload_bytes(
                minio_client,
                minio_settings.landing_bucket,
                spatial_object_key,
                spatial_dataset_bytes,
                content_type="application/json",
            )

            ingest_run_id = _launch_job_with_run_config(
                dagster_client,
                job_name="ingest_job",
                run_config={
                    "ops": {
                        "load_to_postgis": {
                            "inputs": {"manifest": {"value": spatial_manifest}}
                        }
                    }
                },
            )
            status, error_details = _poll_run_to_completion(dagster_client, ingest_run_id)
            if status != "SUCCESS":
                pytest.fail(f"ingest_job failed with status {status}. Details: {error_details}")

            spatial_asset_doc = _assert_mongodb_asset_exists(mongo_client, mongo_settings, ingest_run_id)
            spatial_asset_id = str(spatial_asset_doc["_id"])
            _assert_datalake_object_exists(minio_client, minio_settings.lake_bucket, spatial_asset_doc["s3_key"])

            # 2) Upload tabular CSV matching spatial join key
            join_key, join_value = _extract_join_key_and_value(spatial_dataset_bytes)
            csv_bytes = (f"{join_key},owner_name\n{join_value},Alice\n{join_value},Bob\n").encode("utf-8")
            _upload_bytes(
                minio_client,
                minio_settings.landing_bucket,
                tabular_object_key,
                csv_bytes,
                content_type="text/csv",
            )

            # 3) Launch join_asset_job (asset job) via GraphQL
            join_manifest = {
                "batch_id": batch_id,
                "uploader": "integration_test",
                "intent": "join_datasets",
                "files": [
                    {
                        "path": f"s3://landing-zone/{tabular_object_key}",
                        "type": "tabular",
                        "format": "CSV",
                    }
                ],
                "metadata": {
                    "project": "E2E_JOIN_TEST",
                    "description": "E2E join test",
                    "tags": {"dataset_id": f"joined_{batch_id}"},
                    "join_config": {
                        "target_asset_id": spatial_asset_id,
                        "left_key": join_key,
                        "right_key": join_key,
                        "how": "left",
                    },
                },
            }

            join_run_id = _launch_job_with_run_config(
                dagster_client,
                job_name="join_asset_job",
                run_config={
                    "ops": {"raw_manifest_json": {"config": {"manifest": join_manifest}}}
                },
                partition_key=join_manifest["metadata"]["tags"]["dataset_id"],
            )
            status, error_details = _poll_run_to_completion(dagster_client, join_run_id)
            if status != "SUCCESS":
                pytest.fail(f"join_asset_job failed with status {status}. Details: {error_details}")

            # 4) Validate joined output in Mongo + data lake + lineage
            joined_asset_doc = _assert_mongodb_asset_exists(mongo_client, mongo_settings, join_run_id)
            assert joined_asset_doc.get("kind") == "joined"
            _assert_datalake_object_exists(
                minio_client, minio_settings.lake_bucket, joined_asset_doc["s3_key"]
            )

            db = mongo_client[mongo_settings.database]
            lineage_doc = db["lineage"].find_one(
                {
                    "source_asset_id": ObjectId(spatial_asset_id),
                    "target_asset_id": ObjectId(str(joined_asset_doc["_id"])),
                }
            )
            assert lineage_doc is not None, "Lineage record not found"
            assert lineage_doc["transformation"] == "spatial_join"

            # 5) PostGIS schema cleanup
            _assert_postgis_schema_cleaned(postgis_connection, join_run_id)

        finally:
            _cleanup_landing_zone_object(minio_client, minio_settings.landing_bucket, spatial_object_key)
            _cleanup_landing_zone_object(minio_client, minio_settings.landing_bucket, tabular_object_key)

            # Best-effort cleanup of lake + Mongo docs
            db = mongo_client[mongo_settings.database]
            for doc in [joined_asset_doc, spatial_asset_doc]:
                if not doc:
                    continue
                try:
                    minio_client.remove_object(minio_settings.lake_bucket, doc["s3_key"])
                except Exception:
                    pass
                try:
                    db["assets"].delete_one({"_id": doc["_id"]})
                except Exception:
                    pass

            try:
                ids = []
                if spatial_asset_doc is not None:
                    ids.append(spatial_asset_doc["_id"])
                if joined_asset_doc is not None:
                    ids.append(joined_asset_doc["_id"])
                if ids:
                    db["lineage"].delete_many(
                        {"$or": [{"source_asset_id": {"$in": ids}}, {"target_asset_id": {"$in": ids}}]}
                    )
            except Exception:
                pass


