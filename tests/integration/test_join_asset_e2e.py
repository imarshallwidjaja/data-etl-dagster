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
from pathlib import Path
from typing import Optional
from uuid import uuid4

import pytest
from bson import ObjectId

from libs.spatial_utils import RunIdSchemaMapping

from .helpers import (
    add_dynamic_partition,
    assert_datalake_object_exists,
    assert_mongodb_asset_exists,
    cleanup_minio_object,
    cleanup_mongodb_asset,
    cleanup_mongodb_lineage,
    format_error_details,
    poll_run_to_completion,
    upload_bytes_to_minio,
)


pytestmark = [pytest.mark.integration, pytest.mark.e2e]

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "asset_plans"
SPATIAL_MANIFEST_TEMPLATE_PATH = FIXTURES_DIR / "e2e_spatial_manifest.json"
SPATIAL_DATASET_PATH = FIXTURES_DIR / "e2e_sample_sa1_data.json"
TABULAR_DATASET_PATH = FIXTURES_DIR / "e2e_sample_table_data.csv"
JOIN_MANIFEST_TEMPLATE_PATH = FIXTURES_DIR / "e2e_join_manifest.json"


def _load_fixture_manifest() -> dict:
    return json.loads(SPATIAL_MANIFEST_TEMPLATE_PATH.read_text())


def _load_fixture_dataset() -> bytes:
    return SPATIAL_DATASET_PATH.read_bytes()


def _load_tabular_fixture_dataset() -> bytes:
    return TABULAR_DATASET_PATH.read_bytes()


def _load_join_manifest_template() -> dict:
    return json.loads(JOIN_MANIFEST_TEMPLATE_PATH.read_text())


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
            # 1) Create spatial parent via spatial_asset_job
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

            upload_bytes_to_minio(
                minio_client,
                minio_settings.landing_bucket,
                spatial_object_key,
                spatial_dataset_bytes,
                "application/json",
            )

            add_dynamic_partition(dagster_client, spatial_partition_key)

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
            status, error_details = poll_run_to_completion(
                dagster_client, spatial_run_id
            )
            if status != "SUCCESS":
                pytest.fail(
                    f"spatial_asset_job failed: {status}.{format_error_details(error_details)}"
                )

            spatial_asset_doc = assert_mongodb_asset_exists(
                mongo_client, mongo_settings, spatial_run_id
            )
            spatial_dataset_id = spatial_asset_doc["dataset_id"]
            spatial_object_id = str(spatial_asset_doc["_id"])

            # 2) Create tabular parent via tabular_asset_job
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
                    "title": "E2E Tabular Parent for Join",
                    "description": "Tabular parent dataset for join test",
                    "keywords": ["test", "tabular", "join", "e2e"],
                    "source": "E2E Test Suite",
                    "license": "MIT",
                    "attribution": "Test Runner",
                    "project": "E2E_JOIN_TEST",
                    "tags": {"dataset_id": tabular_partition_key},
                },
            }

            upload_bytes_to_minio(
                minio_client,
                minio_settings.landing_bucket,
                tabular_object_key,
                tabular_dataset_bytes,
                "text/csv",
            )

            add_dynamic_partition(dagster_client, tabular_partition_key)

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
            status, error_details = poll_run_to_completion(
                dagster_client, tabular_run_id
            )
            if status != "SUCCESS":
                pytest.fail(
                    f"tabular_asset_job failed: {status}.{format_error_details(error_details)}"
                )

            tabular_asset_doc = assert_mongodb_asset_exists(
                mongo_client, mongo_settings, tabular_run_id
            )
            tabular_dataset_id = tabular_asset_doc["dataset_id"]
            tabular_object_id = str(tabular_asset_doc["_id"])

            # 3) Launch join_asset_job with both asset IDs (no files)
            join_manifest = _load_join_manifest_template()
            join_manifest = join_manifest.copy()
            join_manifest["batch_id"] = f"join_{batch_id}"
            join_manifest["metadata"]["tags"] = dict(
                join_manifest["metadata"].get("tags", {})
            )
            join_manifest["metadata"]["tags"]["dataset_id"] = join_partition_key
            join_manifest["metadata"]["join_config"]["spatial_dataset_id"] = (
                spatial_dataset_id
            )
            join_manifest["metadata"]["join_config"]["tabular_dataset_id"] = (
                tabular_dataset_id
            )

            add_dynamic_partition(dagster_client, join_partition_key)

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
            status, error_details = poll_run_to_completion(dagster_client, join_run_id)
            if status != "SUCCESS":
                pytest.fail(
                    f"join_asset_job failed: {status}.{format_error_details(error_details)}"
                )

            # 4) Verify joined asset in MongoDB + data-lake
            joined_asset_doc = assert_mongodb_asset_exists(
                mongo_client, mongo_settings, join_run_id
            )
            assert joined_asset_doc.get("kind") == "joined"
            assert_datalake_object_exists(
                minio_client, minio_settings.lake_bucket, joined_asset_doc["s3_key"]
            )

            joined_metadata = joined_asset_doc.get("metadata") or {}
            assert joined_metadata.get("title"), "title should be present and non-empty"
            assert joined_metadata.get("source"), (
                "source should be present and non-empty"
            )
            assert joined_metadata.get("license"), (
                "license should be present and non-empty"
            )
            assert joined_metadata.get("attribution"), (
                "attribution should be present and non-empty"
            )
            assert isinstance(joined_metadata.get("keywords"), list), (
                "keywords should be a list"
            )
            assert joined_metadata.get("geometry_type"), (
                "geometry_type should be captured for joined assets"
            )
            assert isinstance(joined_metadata.get("column_schema"), dict), (
                "column_schema should be a dict"
            )

            # 5) Verify lineage edges (both parents -> joined)
            db = mongo_client[mongo_settings.database]
            joined_id = joined_asset_doc["_id"]

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

            # 6) Verify PostGIS cleanup
            _assert_postgis_schema_cleaned(postgis_connection, join_run_id)

        finally:
            cleanup_minio_object(
                minio_client, minio_settings.landing_bucket, spatial_object_key
            )
            cleanup_minio_object(
                minio_client, minio_settings.landing_bucket, tabular_object_key
            )

            db = mongo_client[mongo_settings.database]
            asset_ids = []
            for doc in [joined_asset_doc, spatial_asset_doc, tabular_asset_doc]:
                if doc is None:
                    continue
                asset_ids.append(doc["_id"])
                cleanup_minio_object(
                    minio_client, minio_settings.lake_bucket, doc.get("s3_key", "")
                )
                cleanup_mongodb_asset(mongo_client, mongo_settings, doc)

            cleanup_mongodb_lineage(mongo_client, mongo_settings, asset_ids)
