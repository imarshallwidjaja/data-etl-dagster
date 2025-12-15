"""Integration test: End-to-end ingest_job pipeline via Dagster GraphQL.

This test validates the full offline-first ETL loop:
1. Upload sample data to landing-zone
2. Launch ingest_job via GraphQL with manifest input
3. Verify data-lake object exists
4. Verify MongoDB asset record exists
5. Verify PostGIS ephemeral schema is cleaned up

Run with: pytest tests/integration/test_ingest_job_e2e.py -v -m "integration and e2e"
"""

import os
import json
import time
import pytest
import psycopg2
import requests
from io import BytesIO
from pathlib import Path
from typing import Optional
from uuid import uuid4
from requests.exceptions import RequestException, Timeout
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient

from libs.models import MinIOSettings, MongoSettings, PostGISSettings
from libs.spatial_utils import RunIdSchemaMapping


pytestmark = [pytest.mark.integration, pytest.mark.e2e]


# Test fixtures directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"
MANIFEST_TEMPLATE_PATH = FIXTURES_DIR / "e2e_sample_sa1_data-manifest.json"
DATASET_PATH = FIXTURES_DIR / "e2e_sample_sa1_data.json"


@pytest.fixture
def dagster_graphql_url():
    """Get Dagster GraphQL endpoint URL."""
    port = os.getenv("DAGSTER_WEBSERVER_PORT", "3000")
    return f"http://localhost:{port}/graphql"


@pytest.fixture
def dagster_client(dagster_graphql_url):
    """Create a simple Dagster GraphQL client."""
    
    class DagsterGraphQLClient:
        def __init__(self, url: str):
            self.url = url
        
        def query(self, query_str: str, variables: Optional[dict] = None, timeout: int = 30) -> dict:
            """Execute a GraphQL query."""
            payload = {"query": query_str}
            if variables:
                payload["variables"] = variables
            
            try:
                response = requests.post(
                    self.url,
                    json=payload,
                    timeout=timeout,
                )
                if response.status_code != 200:
                    raise RuntimeError(
                        f"GraphQL request failed with status {response.status_code}: "
                        f"{response.text}"
                    )
                return response.json()
            except (RequestException, Timeout) as e:
                raise RuntimeError(f"Failed to communicate with Dagster GraphQL API: {e}")
    
    return DagsterGraphQLClient(dagster_graphql_url)


@pytest.fixture
def minio_settings():
    """Load MinIO settings from environment."""
    return MinIOSettings()


@pytest.fixture
def minio_client(minio_settings):
    """Create MinIO client and ensure required buckets exist."""
    client = Minio(
        minio_settings.endpoint,
        access_key=minio_settings.access_key,
        secret_key=minio_settings.secret_key,
        secure=minio_settings.use_ssl,
    )
    
    # Wait for MinIO to be ready
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            client.list_buckets()
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("MinIO did not become ready within timeout")
    
    # Ensure buckets exist
    for bucket in [minio_settings.landing_bucket, minio_settings.lake_bucket]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
    
    return client


@pytest.fixture
def mongo_settings():
    """Load MongoDB settings from environment."""
    return MongoSettings()


@pytest.fixture
def mongo_client(mongo_settings):
    """Create MongoDB client."""
    client = MongoClient(
        mongo_settings.connection_string,
        serverSelectionTimeoutMS=5000,
    )
    return client


@pytest.fixture
def postgis_settings():
    """Load PostGIS settings from environment."""
    return PostGISSettings()


@pytest.fixture
def postgis_connection(postgis_settings):
    """Create PostGIS connection."""
    conn = psycopg2.connect(
        postgis_settings.connection_string,
        connect_timeout=5,
    )
    yield conn
    conn.close()


def _load_fixture_manifest() -> dict:
    """Load and return the manifest template from fixtures."""
    with open(MANIFEST_TEMPLATE_PATH, "r") as f:
        return json.load(f)


def _load_fixture_dataset() -> bytes:
    """Load and return the dataset bytes from fixtures."""
    with open(DATASET_PATH, "rb") as f:
        return f.read()


def _upload_dataset_to_landing_zone(
    minio_client: Minio,
    landing_bucket: str,
    object_key: str,
    dataset_bytes: bytes,
) -> None:
    """Upload dataset to MinIO landing zone."""
    data = BytesIO(dataset_bytes)
    minio_client.put_object(
        landing_bucket,
        object_key,
        data,
        length=len(dataset_bytes),
        content_type="application/json",
    )


def _launch_ingest_job(
    dagster_client,
    manifest: dict,
) -> str:
    """Launch ingest_job via GraphQL and return run_id."""
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
    
    variables = {
        "repositoryLocationName": "etl_pipelines",
        "repositoryName": "__repository__",
        "jobName": "ingest_job",
        "runConfigData": {
            "ops": {
                "load_to_postgis": {
                    "inputs": {
                        "manifest": {
                            "value": manifest
                        }
                    }
                }
            }
        },
    }
    
    result = dagster_client.query(launch_query, variables=variables, timeout=10)
    
    assert "errors" not in result, f"Failed to launch job: {result.get('errors')}"
    
    launch_response = result["data"]["launchRun"]
    assert "run" in launch_response, \
        f"Job launch failed: {launch_response.get('message', 'Unknown error')}"
    
    run_id = launch_response["run"]["runId"]
    assert run_id, "No run_id returned from job launch"
    
    return run_id


def _poll_run_to_completion(
    dagster_client,
    run_id: str,
    max_wait: int = 600,  # 10 minutes for E2E
    poll_interval: int = 3,  # Check every 3 seconds
) -> tuple[str, Optional[dict]]:
    """Poll run until terminal state and return (status, error_details)."""
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
            run_query,
            variables={"runId": run_id},
            timeout=10
        )
        
        assert "errors" not in run_result, \
            f"Failed to query run status: {run_result.get('errors')}"
        
        run_or_error = run_result["data"]["runOrError"]
        if "id" not in run_or_error:
            # Run not found yet, wait and retry
            time.sleep(poll_interval)
            elapsed += poll_interval
            continue
        
        status = run_or_error["status"]
        
        # Check for terminal states
        if status in ["SUCCESS", "FAILURE", "CANCELED"]:
            break
        
        time.sleep(poll_interval)
        elapsed += poll_interval
    
    # If not successful, fetch error details
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
            log_query,
            variables={"runId": run_id},
            timeout=10
        )
        
        if "data" in log_result and "logsForRun" in log_result["data"]:
            logs = log_result["data"]["logsForRun"]
            if logs.get("__typename") == "EventConnection":
                events_list = logs.get("events", [])
                error_events = [
                    e for e in events_list 
                    if e.get("level") == "ERROR" or "Failure" in e.get("__typename", "")
                ]
                if error_events:
                    error_details = error_events
                else:
                    error_details = events_list[-10:] if events_list else "No events found"
    
    return status, error_details


def _assert_mongodb_asset_exists(
    mongo_client: MongoClient,
    mongo_settings: MongoSettings,
    run_id: str,
) -> dict:
    """Assert MongoDB asset record exists and return the asset document."""
    db = mongo_client[mongo_settings.database]
    collection = db["assets"]
    
    asset_doc = collection.find_one({"dagster_run_id": run_id})
    assert asset_doc is not None, \
        f"No asset record found in MongoDB for run_id: {run_id}"
    
    return asset_doc


def _assert_datalake_object_exists(
    minio_client: Minio,
    lake_bucket: str,
    s3_key: str,
) -> None:
    """Assert MinIO data-lake object exists and has size > 0."""
    try:
        stat = minio_client.stat_object(lake_bucket, s3_key)
        assert stat.size > 0, f"Data-lake object {s3_key} has zero size"
    except S3Error as e:
        pytest.fail(f"Data-lake object {s3_key} does not exist: {e}")


def _assert_postgis_schema_cleaned(
    postgis_connection,
    run_id: str,
) -> None:
    """Assert PostGIS ephemeral schema does not exist."""
    schema_mapping = RunIdSchemaMapping.from_run_id(run_id)
    schema_name = schema_mapping.schema_name
    
    with postgis_connection.cursor() as cur:
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = %s
        """, (schema_name,))
        result = cur.fetchone()
        
        assert result is None, \
            f"PostGIS schema {schema_name} still exists after job completion (should be cleaned up)"


def _cleanup_landing_zone_object(
    minio_client: Minio,
    landing_bucket: str,
    object_key: str,
) -> None:
    """Remove uploaded landing-zone object."""
    try:
        minio_client.remove_object(landing_bucket, object_key)
    except S3Error:
        pass  # Ignore cleanup errors


def _cleanup_asset_artifacts(
    minio_client: Minio,
    mongo_client: MongoClient,
    mongo_settings: MongoSettings,
    lake_bucket: str,
    run_id: str,
    asset_doc: Optional[dict],
) -> None:
    """Clean up asset artifacts (data-lake object and MongoDB record)."""
    if asset_doc is None:
        return
    
    # Delete data-lake object
    s3_key = asset_doc.get("s3_key")
    if s3_key:
        try:
            minio_client.remove_object(lake_bucket, s3_key)
        except S3Error:
            pass  # Ignore cleanup errors
    
    # Delete MongoDB asset record
    db = mongo_client[mongo_settings.database]
    collection = db["assets"]
    try:
        collection.delete_many({"dagster_run_id": run_id})
    except Exception:
        pass  # Ignore cleanup errors


class TestIngestJobE2E:
    """End-to-end test suite for ingest_job pipeline."""
    
    def test_ingest_job_full_pipeline(
        self,
        dagster_client,
        minio_client,
        minio_settings,
        mongo_client,
        mongo_settings,
        postgis_connection,
        postgis_settings,
    ):
        """
        Test full ingest_job pipeline: landing-zone → PostGIS → data-lake + MongoDB.
        
        This test:
        1. Uploads sample data to landing-zone
        2. Launches ingest_job via GraphQL with manifest input
        3. Polls for job completion
        4. Asserts MongoDB asset record exists
        5. Asserts data-lake object exists
        6. Asserts PostGIS schema is cleaned up
        7. Cleans up test artifacts
        """
        # Generate unique batch_id and object key
        batch_id = f"e2e_{uuid4().hex[:12]}"
        object_key = f"e2e/{batch_id}/input.json"
        
        # Load fixture files
        manifest_template = _load_fixture_manifest()
        dataset_bytes = _load_fixture_dataset()
        
        # Prepare manifest with unique batch_id and object path
        manifest = manifest_template.copy()
        manifest["batch_id"] = batch_id
        manifest["files"][0]["path"] = f"s3://landing-zone/{object_key}"
        
        # Track run_id and asset doc for cleanup
        run_id = None
        asset_doc = None
        
        try:
            # 1. Upload dataset to landing-zone
            _upload_dataset_to_landing_zone(
                minio_client,
                minio_settings.landing_bucket,
                object_key,
                dataset_bytes,
            )
            
            # 2. Launch ingest_job via GraphQL
            run_id = _launch_ingest_job(dagster_client, manifest)
            print(f"Launched ingest_job run: {run_id}")
            
            # 3. Poll for job completion
            status, error_details = _poll_run_to_completion(dagster_client, run_id)
            
            # 4. Assert job succeeded
            if status != "SUCCESS":
                error_msg = f"Job failed with status {status}"
                if error_details:
                    error_msg += f". Error details: {error_details}"
                pytest.fail(error_msg)
            
            print(f"✅ Job completed successfully: {run_id}")
            
            # 5. Assert MongoDB asset record exists
            asset_doc = _assert_mongodb_asset_exists(
                mongo_client,
                mongo_settings,
                run_id,
            )
            print(f"✅ MongoDB asset record found: {asset_doc.get('_id')}")
            
            # 6. Assert data-lake object exists
            s3_key = asset_doc.get("s3_key")
            assert s3_key, "Asset document missing s3_key"
            _assert_datalake_object_exists(
                minio_client,
                minio_settings.lake_bucket,
                s3_key,
            )
            print(f"✅ Data-lake object exists: {s3_key}")
            
            # 7. Assert PostGIS schema is cleaned up
            _assert_postgis_schema_cleaned(postgis_connection, run_id)
            print(f"✅ PostGIS schema cleaned up for run: {run_id}")
            
        finally:
            # 8. Cleanup (always, even on failure)
            _cleanup_landing_zone_object(
                minio_client,
                minio_settings.landing_bucket,
                object_key,
            )
            
            # Only cleanup asset artifacts if run_id was assigned
            if run_id is not None:
                _cleanup_asset_artifacts(
                    minio_client,
                    mongo_client,
                    mongo_settings,
                    minio_settings.lake_bucket,
                    run_id,
                    asset_doc,
                )
            
            print("✅ Cleanup completed")
