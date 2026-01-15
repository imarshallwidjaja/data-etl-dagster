"""Integration test: End-to-end ingest_job pipeline via Dagster GraphQL.

This test validates the full offline-first ETL loop:
1. Upload sample data to landing-zone
2. Launch ingest_job via GraphQL with manifest input
3. Verify data-lake object exists
4. Verify MongoDB asset record exists
5. Verify PostGIS ephemeral schema is cleaned up

Run with: pytest tests/integration/test_ingest_job_e2e.py -v -m "integration and e2e"
"""

import json
from pathlib import Path
from typing import Optional
from uuid import uuid4

import pytest

from libs.spatial_utils import RunIdSchemaMapping

from .helpers import (
    assert_datalake_object_exists,
    assert_mongodb_asset_exists_legacy,
    cleanup_minio_object,
    format_error_details,
    poll_run_to_completion,
    upload_bytes_to_minio,
)


pytestmark = [pytest.mark.integration, pytest.mark.e2e]

FIXTURES_DIR = Path(__file__).parent / "fixtures"
MANIFEST_TEMPLATE_PATH = FIXTURES_DIR / "e2e_sample_sa1_data-manifest.json"
DATASET_PATH = FIXTURES_DIR / "e2e_sample_sa1_data.json"


def _load_fixture_manifest() -> dict:
    with open(MANIFEST_TEMPLATE_PATH, "r") as f:
        return json.load(f)


def _load_fixture_dataset() -> bytes:
    with open(DATASET_PATH, "rb") as f:
        return f.read()


def _launch_ingest_job(dagster_client, manifest: dict) -> str:
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
            "ops": {"load_to_postgis": {"inputs": {"manifest": {"value": manifest}}}}
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


def _cleanup(
    minio_client,
    minio_settings,
    mongo_client,
    mongo_settings,
    landing_key: str,
    asset_doc: Optional[dict],
) -> None:
    cleanup_minio_object(minio_client, minio_settings.landing_bucket, landing_key)

    if asset_doc is None:
        return

    s3_key = asset_doc.get("s3_key")
    if s3_key:
        cleanup_minio_object(minio_client, minio_settings.lake_bucket, s3_key)

    try:
        db = mongo_client[mongo_settings.database]
        db["assets"].delete_one({"_id": asset_doc["_id"]})
    except Exception:
        pass


class TestIngestJobE2E:
    def test_ingest_job_full_pipeline(
        self,
        dagster_client,
        minio_client,
        minio_settings,
        mongo_client,
        mongo_settings,
        postgis_connection,
    ):
        batch_id = f"e2e_{uuid4().hex[:12]}"
        object_key = f"e2e/{batch_id}/input.json"

        manifest_template = _load_fixture_manifest()
        dataset_bytes = _load_fixture_dataset()

        manifest = manifest_template.copy()
        manifest["batch_id"] = batch_id
        manifest["files"][0]["path"] = f"s3://landing-zone/{object_key}"

        run_id = None
        asset_doc = None

        try:
            upload_bytes_to_minio(
                minio_client,
                minio_settings.landing_bucket,
                object_key,
                dataset_bytes,
                "application/json",
            )

            run_id = _launch_ingest_job(dagster_client, manifest)
            print(f"Launched ingest_job run: {run_id}")

            status, error_details = poll_run_to_completion(dagster_client, run_id)

            if status != "SUCCESS":
                pytest.fail(
                    f"ingest_job failed: {status}.{format_error_details(error_details)}"
                )

            print(f"Job completed successfully: {run_id}")

            asset_doc = assert_mongodb_asset_exists_legacy(
                mongo_client,
                mongo_settings,
                run_id,
            )
            print(f"MongoDB asset record found: {asset_doc.get('_id')}")

            s3_key = asset_doc.get("s3_key")
            assert isinstance(s3_key, str), "Asset document missing s3_key"
            assert_datalake_object_exists(
                minio_client,
                minio_settings.lake_bucket,
                s3_key,
            )
            print(f"Data-lake object exists: {s3_key}")

            _assert_postgis_schema_cleaned(postgis_connection, run_id)
            print(f"PostGIS schema cleaned up for run: {run_id}")

        finally:
            _cleanup(
                minio_client,
                minio_settings,
                mongo_client,
                mongo_settings,
                object_key,
                asset_doc,
            )

            print("Cleanup completed")
