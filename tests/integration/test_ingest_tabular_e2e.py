"""Integration test: End-to-end ingest_tabular_job pipeline via Dagster GraphQL.

This test validates the tabular offline-first ETL loop:
1. Upload a sample CSV to landing-zone
2. Launch ingest_tabular_job via GraphQL with manifest input
3. Verify data-lake Parquet object exists
4. Verify MongoDB asset record exists and has tabular-specific fields
5. Cleanup MinIO + Mongo artifacts

Run with: pytest tests/integration/test_ingest_tabular_e2e.py -v -m "integration and e2e"
"""

import os
import time
import pytest
import requests
from io import BytesIO
from typing import Optional
from uuid import uuid4
from requests.exceptions import RequestException, Timeout
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient

from libs.models import MinIOSettings, MongoSettings


pytestmark = [pytest.mark.integration, pytest.mark.e2e]


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
    return b"Col A,Col B\n1,hello\n2,world\n"


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


def _launch_ingest_tabular_job(dagster_client, manifest: dict) -> str:
    launch_query = """
    mutation LaunchRun(
        $repositoryLocationName: String!
        $repositoryName: String!
        $jobName: String!
        $runConfigData: RunConfigData
    ) {
        launchRun(
            executionParams: {
                selector: {
                    repositoryLocationName: $repositoryLocationName
                    repositoryName: $repositoryName
                    pipelineName: $jobName
                }
                runConfigData: $runConfigData
            }
        ) {
            ... on LaunchRunSuccess {
                run {
                    runId
                    status
                }
            }
            ... on PipelineNotFoundError {
                message
            }
            ... on RunConfigValidationInvalid {
                errors {
                    message
                }
            }
            ... on PythonError {
                message
            }
        }
    }
    """

    # IMPORTANT: For this job/op wiring, Dagster expects the manifest dict directly.
    # If we wrap it as {"value": manifest}, the op receives {"value": {...}} and Pydantic rejects it.
    variables = {
        "repositoryLocationName": "etl_pipelines",
        "repositoryName": "__repository__",
        "jobName": "ingest_tabular_job",
        "runConfigData": {
            "ops": {"download_tabular_from_landing": {"inputs": {"manifest": manifest}}}
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
                ... on Run {
                    id
                    status
                }
                ... on RunNotFoundError {
                    message
                }
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
        log_query = """
        query GetRunLogs($runId: ID!) {
            logsForRun(runId: $runId) {
                __typename
                ... on EventConnection {
                    events {
                        __typename
                        ... on MessageEvent {
                            message
                            level
                        }
                        ... on ExecutionStepFailureEvent {
                            error {
                                message
                                stack
                            }
                        }
                    }
                }
            }
        }
        """

        log_result = dagster_client.query(
            log_query, variables={"runId": run_id}, timeout=10
        )
        if "data" in log_result and "logsForRun" in log_result["data"]:
            logs = log_result["data"]["logsForRun"]
            if logs.get("__typename") == "EventConnection":
                events_list = logs.get("events", [])
                error_events = [
                    e
                    for e in events_list
                    if e.get("level") == "ERROR"
                    or "Failure" in e.get("__typename", "")
                ]
                error_details = error_events or (
                    events_list[-10:] if events_list else None
                )

    return status, error_details


def _assert_mongodb_asset_exists(
    mongo_client: MongoClient,
    mongo_settings: MongoSettings,
    run_id: str,
) -> dict:
    db = mongo_client[mongo_settings.database]
    collection = db["assets"]

    asset_doc = collection.find_one({"dagster_run_id": run_id})
    assert asset_doc is not None, f"No asset record found in MongoDB for run_id: {run_id}"
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


class TestIngestTabularJobE2E:
    def test_ingest_tabular_job_full_pipeline(
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
        manifest = {
            "batch_id": batch_id,
            "uploader": "integration_test",
            "intent": "ingest_tabular",
            "files": [
                {
                    "path": f"s3://landing-zone/{object_key}",
                    "type": "tabular",
                    "format": "CSV",
                }
            ],
            "metadata": {
                "project": "test_tabular",
                "description": "E2E tabular ingestion test",
                "tags": {"priority": 1},
            },
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

            run_id = _launch_ingest_tabular_job(dagster_client, manifest)

            status, error_details = _poll_run_to_completion(dagster_client, run_id)
            if status != "SUCCESS":
                error_msg = f"Job failed with status {status}"
                if error_details:
                    error_msg += f". Error details: {error_details}"
                pytest.fail(error_msg)

            asset_doc = _assert_mongodb_asset_exists(mongo_client, mongo_settings, run_id)

            assert asset_doc.get("kind") == "tabular"
            assert asset_doc.get("format") == "parquet"
            assert "crs" not in asset_doc or asset_doc.get("crs") is None
            assert "bounds" not in asset_doc or asset_doc.get("bounds") is None

            metadata = asset_doc.get("metadata") or {}
            tags = metadata.get("tags") or {}
            assert tags.get("priority") == 1

            header_mapping = metadata.get("header_mapping")
            assert isinstance(header_mapping, dict)
            assert header_mapping.get("Col A") == "col_a"
            assert header_mapping.get("Col B") == "col_b"

            s3_key = asset_doc.get("s3_key")
            assert s3_key, "Asset document missing s3_key"
            _assert_datalake_object_exists(
                minio_client, minio_settings.lake_bucket, s3_key
            )

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

    def test_ingest_tabular_job_with_join_key_normalization(
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
        manifest = {
            "batch_id": batch_id,
            "uploader": "integration_test",
            "intent": "ingest_tabular",
            "files": [
                {
                    "path": f"s3://landing-zone/{object_key}",
                    "type": "tabular",
                    "format": "CSV",
                }
            ],
            "metadata": {
                "project": "test_tabular",
                "description": "E2E tabular join-key normalization test",
                "tags": {"priority": 1},
                "join_config": {
                    "left_key": "Col A",
                    "right_key": "Col A",
                    "how": "left",
                    "target_asset_id": "dataset_placeholder",
                },
            },
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

            run_id = _launch_ingest_tabular_job(dagster_client, manifest)

            status, error_details = _poll_run_to_completion(dagster_client, run_id)
            if status != "SUCCESS":
                error_msg = f"Job failed with status {status}"
                if error_details:
                    error_msg += f". Error details: {error_details}"
                pytest.fail(error_msg)

            asset_doc = _assert_mongodb_asset_exists(mongo_client, mongo_settings, run_id)
            metadata = asset_doc.get("metadata") or {}
            tags = metadata.get("tags") or {}
            assert tags.get("join_key_clean") == "col_a"

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


