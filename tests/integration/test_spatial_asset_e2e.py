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
from pathlib import Path
from uuid import uuid4

import pytest

from libs.spatial_utils import RunIdSchemaMapping

from .helpers import (
    add_dynamic_partition,
    assert_datalake_object_exists,
    assert_parquet_valid,
    cleanup_dynamic_partitions,
    cleanup_minio_object,
    format_error_details,
    poll_run_to_completion,
    upload_bytes_to_minio,
)


pytestmark = [pytest.mark.integration, pytest.mark.e2e]

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "asset_plans"
MANIFEST_TEMPLATE_PATH = FIXTURES_DIR / "e2e_spatial_manifest.json"
DATASET_PATH = FIXTURES_DIR / "e2e_sample_sa1_data.json"


def _load_fixture_manifest() -> dict:
    return json.loads(MANIFEST_TEMPLATE_PATH.read_text())


def _load_fixture_dataset() -> bytes:
    return DATASET_PATH.read_bytes()


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


class TestSpatialAssetE2E:
    def test_spatial_asset_job_full_pipeline(
        self,
        dagster_client,
        minio_client,
        minio_landing_bucket,
        minio_lake_bucket,
        mongo_client,
        mongo_database,
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
        manifest["metadata"]["tags"] = dict(
            manifest.get("metadata", {}).get("tags", {})
        )
        manifest["metadata"]["tags"]["dataset_id"] = partition_key

        run_id: str | None = None
        asset_doc: dict | None = None
        created_partitions: set[str] = set()
        test_error: BaseException | None = None

        try:
            upload_bytes_to_minio(
                minio_client,
                minio_landing_bucket,
                landing_key,
                dataset_bytes,
                "application/json",
            )

            add_dynamic_partition(dagster_client, partition_key)
            created_partitions.add(partition_key)

            run_id = _launch_spatial_asset_job(
                dagster_client, manifest=manifest, partition_key=partition_key
            )

            status, error_details = poll_run_to_completion(dagster_client, run_id)
            if status != "SUCCESS":
                pytest.fail(
                    f"spatial_asset_job failed: {status}.{format_error_details(error_details)}"
                )

            asset_doc = assert_mongodb_asset_exists(
                mongo_client, mongo_database, run_id
            )
            assert asset_doc.get("kind") == "spatial", (
                f"Expected kind=spatial, got {asset_doc.get('kind')}"
            )

            metadata = asset_doc.get("metadata") or {}
            column_schema = metadata.get("column_schema")
            assert isinstance(column_schema, dict)
            assert "sa1_code21" in column_schema
            assert column_schema["sa1_code21"]["type_name"] == "INTEGER"
            assert "geom" in column_schema
            assert column_schema["geom"]["type_name"] == "GEOMETRY"

            geometry_type = metadata.get("geometry_type")
            assert geometry_type is not None, "geometry_type should be captured"
            assert isinstance(geometry_type, str), "geometry_type should be a string"
            assert geometry_type in ["MULTIPOLYGON", "POLYGON", "GEOMETRY"], (
                f"Expected polygon-like geometry type, got {geometry_type}"
            )

            assert metadata.get("title"), "title should be present and non-empty"
            assert metadata.get("source"), "source should be present and non-empty"
            assert metadata.get("license"), "license should be present and non-empty"
            assert metadata.get("attribution"), (
                "attribution should be present and non-empty"
            )
            assert isinstance(metadata.get("keywords"), list), (
                "keywords should be a list"
            )

            assert_datalake_object_exists(
                minio_client, minio_lake_bucket, asset_doc["s3_key"]
            )

            assert_parquet_valid(
                minio_client,
                minio_lake_bucket,
                asset_doc["s3_key"],
                expected_columns=["geom", "sa1_code21"],
            )

            _assert_postgis_schema_cleaned(postgis_connection, run_id)

        except BaseException as e:
            test_error = e
            raise

        finally:
            cleanup_minio_object(minio_client, minio_landing_bucket, landing_key)

            if asset_doc:
                cleanup_minio_object(
                    minio_client, minio_lake_bucket, asset_doc.get("s3_key", "")
                )
                cleanup_mongodb_asset_by_id(mongo_client, mongo_database, asset_doc)

            cleanup_dynamic_partitions(
                dagster_client, created_partitions, original_error=test_error
            )


def cleanup_mongodb_asset_by_id(
    mongo_client, mongo_database: str, asset_doc: dict | None
) -> None:
    if asset_doc is None:
        return
    try:
        db = mongo_client[mongo_database]
        db["assets"].delete_one({"_id": asset_doc["_id"]})
    except Exception:
        pass


def assert_mongodb_asset_exists(
    mongo_client, mongo_database: str, dagster_run_id: str
) -> dict:
    db = mongo_client[mongo_database]
    run_doc = db["runs"].find_one({"dagster_run_id": dagster_run_id})
    assert run_doc is not None, (
        f"No run document found in MongoDB for dagster_run_id: {dagster_run_id}"
    )
    mongodb_run_id = str(run_doc["_id"])
    asset_doc = db["assets"].find_one({"run_id": mongodb_run_id})
    assert asset_doc is not None, (
        f"No asset record found in MongoDB for run_id: {mongodb_run_id} "
        f"(Dagster run: {dagster_run_id})"
    )
    return asset_doc
