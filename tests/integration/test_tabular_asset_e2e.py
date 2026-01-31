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
from pathlib import Path
from uuid import uuid4

import pytest

from .helpers import (
    add_dynamic_partition,
    assert_datalake_object_exists,
    assert_parquet_valid,
    build_test_run_tags,
    cleanup_dynamic_partitions,
    cleanup_mongodb_activity_logs,
    cleanup_mongodb_manifest,
    cleanup_mongodb_run,
    cleanup_minio_object,
    format_error_details,
    poll_run_to_completion,
    upload_bytes_to_minio,
)


pytestmark = [pytest.mark.integration, pytest.mark.e2e]

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "asset_plans"
TABULAR_DATASET_PATH = FIXTURES_DIR / "e2e_sample_table_data.csv"
TABULAR_MANIFEST_TEMPLATE_PATH = FIXTURES_DIR / "e2e_tabular_manifest.json"


def _create_dummy_csv() -> bytes:
    return TABULAR_DATASET_PATH.read_bytes()


def _load_manifest_template() -> dict:
    return json.loads(TABULAR_MANIFEST_TEMPLATE_PATH.read_text())


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
            "tags": build_test_run_tags(
                partition_key=manifest["metadata"]["tags"]["dataset_id"],
                batch_id=manifest.get("batch_id"),
                test_run_id=manifest.get("batch_id"),
            )
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


def _cleanup_minio_mongo(
    minio_client,
    minio_settings,
    mongo_client,
    mongo_settings,
    landing_key: str,
    batch_id: str,
    run_id: str | None,
    asset_doc: dict | None,
) -> None:
    cleanup_minio_object(minio_client, minio_settings.landing_bucket, landing_key)

    cleanup_mongodb_manifest(mongo_client, mongo_settings, batch_id)
    if run_id:
        cleanup_mongodb_activity_logs(mongo_client, mongo_settings, run_id)
        cleanup_mongodb_run(mongo_client, mongo_settings, run_id)

    if asset_doc:
        cleanup_minio_object(
            minio_client, minio_settings.lake_bucket, asset_doc.get("s3_key", "")
        )
        try:
            db = mongo_client[mongo_settings.database]
            db["assets"].delete_one({"_id": asset_doc["_id"]})
        except Exception:
            pass


def _assert_mongodb_asset_exists(
    mongo_client, mongo_settings, dagster_run_id: str
) -> dict:
    db = mongo_client[mongo_settings.database]
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
        manifest = _load_manifest_template()
        manifest["batch_id"] = batch_id
        manifest["files"][0]["path"] = f"s3://landing-zone/{object_key}"
        manifest["metadata"]["tags"] = dict(manifest["metadata"].get("tags", {}))
        manifest["metadata"]["tags"]["dataset_id"] = f"dataset_{batch_id}"
        manifest["metadata"]["tags"]["priority"] = 1

        run_id: str | None = None
        asset_doc: dict | None = None
        created_partitions: set[str] = set()
        test_error: BaseException | None = None

        try:
            upload_bytes_to_minio(
                minio_client,
                minio_settings.landing_bucket,
                object_key,
                csv_bytes,
                "text/csv",
            )

            partition_key = manifest["metadata"]["tags"]["dataset_id"]
            add_dynamic_partition(dagster_client, partition_key)
            created_partitions.add(partition_key)

            run_id = _launch_tabular_asset_job(dagster_client, manifest)

            status, error_details = poll_run_to_completion(dagster_client, run_id)
            if status != "SUCCESS":
                pytest.fail(
                    f"tabular_asset_job failed: {status}.{format_error_details(error_details)}"
                )

            asset_doc = _assert_mongodb_asset_exists(
                mongo_client, mongo_settings, run_id
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
            assert header_mapping.get("ogc_fid") == "ogc_fid"
            assert header_mapping.get("sa1_code21") == "sa1_code21"

            column_schema = metadata.get("column_schema")
            assert isinstance(column_schema, dict)
            assert "ogc_fid" in column_schema
            assert column_schema["ogc_fid"]["type_name"] == "INTEGER"
            assert "sa1_code21" in column_schema
            assert column_schema["sa1_code21"]["type_name"] == "INTEGER"

            assert metadata.get("title"), "title should be present and non-empty"
            assert metadata.get("source"), "source should be present and non-empty"
            assert metadata.get("license"), "license should be present and non-empty"
            assert metadata.get("attribution"), (
                "attribution should be present and non-empty"
            )
            assert isinstance(metadata.get("keywords"), list), (
                "keywords should be a list"
            )

            s3_key = asset_doc.get("s3_key")
            assert s3_key, "Asset document missing s3_key"
            assert_datalake_object_exists(
                minio_client,
                minio_settings.lake_bucket,
                s3_key,
            )

            assert_parquet_valid(
                minio_client,
                minio_settings.lake_bucket,
                s3_key,
                expected_columns=["ogc_fid", "sa1_code21"],
            )

        except BaseException as e:
            test_error = e
            raise

        finally:
            _cleanup_minio_mongo(
                minio_client,
                minio_settings,
                mongo_client,
                mongo_settings,
                object_key,
                batch_id,
                run_id,
                asset_doc,
            )
            cleanup_dynamic_partitions(
                dagster_client, created_partitions, original_error=test_error
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
        manifest = _load_manifest_template()
        manifest["batch_id"] = batch_id
        manifest["files"][0]["path"] = f"s3://landing-zone/{object_key}"
        manifest["metadata"]["tags"] = dict(manifest["metadata"].get("tags", {}))
        manifest["metadata"]["tags"]["dataset_id"] = f"dataset_{batch_id}"
        manifest["metadata"]["tags"]["priority"] = 1
        manifest["metadata"]["join_config"] = {
            "spatial_dataset_id": "placeholder_spatial",
            "tabular_dataset_id": "placeholder_tabular",
            "left_key": "sa1_code21",
            "right_key": "sa1_code21",
            "how": "left",
        }

        run_id: str | None = None
        asset_doc: dict | None = None
        created_partitions: set[str] = set()
        test_error: BaseException | None = None

        try:
            upload_bytes_to_minio(
                minio_client,
                minio_settings.landing_bucket,
                object_key,
                csv_bytes,
                "text/csv",
            )

            partition_key = manifest["metadata"]["tags"]["dataset_id"]
            add_dynamic_partition(dagster_client, partition_key)
            created_partitions.add(partition_key)

            run_id = _launch_tabular_asset_job(dagster_client, manifest)

            status, error_details = poll_run_to_completion(dagster_client, run_id)
            if status != "SUCCESS":
                pytest.fail(
                    f"tabular_asset_job failed: {status}.{format_error_details(error_details)}"
                )

            asset_doc = _assert_mongodb_asset_exists(
                mongo_client, mongo_settings, run_id
            )
            metadata = asset_doc.get("metadata") or {}
            tags = metadata.get("tags") or {}
            assert tags.get("join_key_clean") == "sa1_code21"

        except BaseException as e:
            test_error = e
            raise

        finally:
            _cleanup_minio_mongo(
                minio_client,
                minio_settings,
                mongo_client,
                mongo_settings,
                object_key,
                batch_id,
                run_id,
                asset_doc,
            )
            cleanup_dynamic_partitions(
                dagster_client, created_partitions, original_error=test_error
            )
