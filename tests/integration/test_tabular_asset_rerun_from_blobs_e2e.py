"""Integration test: rerun tabular_asset_job from blob paths without duplicates.

This test validates the tabular offline-first ETL rerun flow:
1. Upload a sample CSV to landing-zone
2. Launch tabular_asset_job via GraphQL with manifest input
3. Capture blob key + content_hash from raw_source artifacts
4. Launch tabular_asset_job again using s3://<data-lake>/<blob.key>
5. Assert the blob document is not duplicated and artifacts increase

Run with: pytest tests/integration/test_tabular_asset_rerun_from_blobs_e2e.py -v -m "integration and e2e"
"""

import json
from pathlib import Path
from uuid import uuid4

import pytest
from bson import ObjectId, errors as bson_errors

from .helpers import (
    add_dynamic_partition,
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


def _load_raw_source_artifacts(
    mongo_client, mongo_settings, batch_id: str
) -> list[dict]:
    db = mongo_client[mongo_settings.database]
    return list(db["artifacts"].find({"batch_id": batch_id, "kind": "raw_source"}))


def _fetch_blob_document(mongo_client, mongo_settings, blob_id: str) -> dict:
    db = mongo_client[mongo_settings.database]
    try:
        blob_object_id = ObjectId(blob_id)
    except bson_errors.InvalidId as exc:
        raise AssertionError(f"Invalid blob_id value: {blob_id}") from exc
    blob_doc = db["blobs"].find_one({"_id": blob_object_id})
    assert blob_doc is not None, f"No blob found for blob_id={blob_id}"
    return blob_doc


def _cleanup_minio_mongo(
    minio_client,
    minio_settings,
    mongo_client,
    mongo_settings,
    landing_key: str,
    batch_ids: list[str],
    run_ids: list[str],
    asset_docs: list[dict],
    blob_doc: dict | None,
) -> None:
    cleanup_minio_object(minio_client, minio_settings.landing_bucket, landing_key)

    for batch_id in batch_ids:
        cleanup_mongodb_manifest(mongo_client, mongo_settings, batch_id)

    for run_id in run_ids:
        cleanup_mongodb_activity_logs(mongo_client, mongo_settings, run_id)
        cleanup_mongodb_run(mongo_client, mongo_settings, run_id)

    for asset_doc in asset_docs:
        cleanup_minio_object(
            minio_client, minio_settings.lake_bucket, asset_doc.get("s3_key", "")
        )
        try:
            db = mongo_client[mongo_settings.database]
            db["assets"].delete_one({"_id": asset_doc["_id"]})
        except Exception:
            pass

    try:
        db = mongo_client[mongo_settings.database]
        db["artifacts"].delete_many({"batch_id": {"$in": batch_ids}})
    except Exception:
        pass

    if blob_doc:
        blob_bucket = blob_doc.get("bucket")
        blob_key = blob_doc.get("key")
        if blob_bucket and blob_key:
            cleanup_minio_object(minio_client, blob_bucket, blob_key)


class TestTabularAssetRerunFromBlobsE2E:
    def test_rerun_from_blob_path_does_not_duplicate_blobs(
        self,
        dagster_client,
        minio_client,
        minio_settings,
        mongo_client,
        mongo_settings,
    ):
        dataset_id = f"dataset_{uuid4().hex[:12]}"
        batch_id_a = f"e2e_tabular_blob_a_{uuid4().hex[:12]}"
        batch_id_b = f"e2e_tabular_blob_b_{uuid4().hex[:12]}"
        object_key = f"e2e/{batch_id_a}/data.csv"

        csv_bytes = _create_dummy_csv()

        manifest_a = _load_manifest_template()
        manifest_a["batch_id"] = batch_id_a
        manifest_a["files"][0]["path"] = (
            f"s3://{minio_settings.landing_bucket}/{object_key}"
        )
        manifest_a["metadata"]["tags"] = dict(manifest_a["metadata"].get("tags", {}))
        manifest_a["metadata"]["tags"]["dataset_id"] = dataset_id
        manifest_a["metadata"]["tags"]["priority"] = 1

        manifest_b = _load_manifest_template()
        manifest_b["batch_id"] = batch_id_b
        manifest_b["metadata"]["tags"] = dict(manifest_b["metadata"].get("tags", {}))
        manifest_b["metadata"]["tags"]["dataset_id"] = dataset_id
        manifest_b["metadata"]["tags"]["priority"] = 1

        run_id_a: str | None = None
        run_id_b: str | None = None
        asset_doc_a: dict | None = None
        asset_doc_b: dict | None = None
        blob_doc: dict | None = None
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

            add_dynamic_partition(dagster_client, dataset_id)
            created_partitions.add(dataset_id)

            run_id_a = _launch_tabular_asset_job(dagster_client, manifest_a)

            status, error_details = poll_run_to_completion(dagster_client, run_id_a)
            if status != "SUCCESS":
                pytest.fail(
                    f"tabular_asset_job failed: {status}.{format_error_details(error_details)}"
                )

            asset_doc_a = _assert_mongodb_asset_exists(
                mongo_client, mongo_settings, run_id_a
            )

            raw_artifacts_a = _load_raw_source_artifacts(
                mongo_client, mongo_settings, batch_id_a
            )
            assert raw_artifacts_a, (
                f"No raw_source artifacts found for batch_id={batch_id_a}"
            )

            blob_ids = {artifact.get("blob_id") for artifact in raw_artifacts_a}
            assert all(blob_ids), "raw_source artifacts missing blob_id"
            assert len(blob_ids) == 1, (
                "Expected single blob_id for single-file manifest"
            )

            blob_id = next(iter(blob_ids))
            blob_doc = _fetch_blob_document(mongo_client, mongo_settings, blob_id)
            blob_key = blob_doc.get("key")
            blob_bucket = blob_doc.get("bucket")
            content_hash = blob_doc.get("content_hash")
            assert blob_key, "Blob document missing key"
            assert blob_bucket, "Blob document missing bucket"
            assert content_hash, "Blob document missing content_hash"

            initial_artifact_count = mongo_client[mongo_settings.database][
                "artifacts"
            ].count_documents({"kind": "raw_source", "blob_id": blob_id})

            manifest_b["files"][0]["path"] = f"s3://{blob_bucket}/{blob_key}"

            run_id_b = _launch_tabular_asset_job(dagster_client, manifest_b)

            status, error_details = poll_run_to_completion(dagster_client, run_id_b)
            if status != "SUCCESS":
                pytest.fail(
                    f"tabular_asset_job rerun failed: {status}.{format_error_details(error_details)}"
                )

            asset_doc_b = _assert_mongodb_asset_exists(
                mongo_client, mongo_settings, run_id_b
            )

            raw_artifacts_b = _load_raw_source_artifacts(
                mongo_client, mongo_settings, batch_id_b
            )
            assert raw_artifacts_b, (
                f"No raw_source artifacts found for batch_id={batch_id_b}"
            )

            updated_artifact_count = mongo_client[mongo_settings.database][
                "artifacts"
            ].count_documents({"kind": "raw_source", "blob_id": blob_id})
            assert updated_artifact_count > initial_artifact_count, (
                "Expected raw_source artifacts to increase for reused blob"
            )

            blob_docs = list(
                mongo_client[mongo_settings.database]["blobs"].find(
                    {"content_hash": content_hash}
                )
            )
            assert len(blob_docs) == 1, (
                "Expected single blob document for reused content_hash"
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
                [batch_id_a, batch_id_b],
                [run_id for run_id in [run_id_a, run_id_b] if run_id],
                [asset_doc for asset_doc in [asset_doc_a, asset_doc_b] if asset_doc],
                blob_doc,
            )
            cleanup_dynamic_partitions(
                dagster_client, created_partitions, original_error=test_error
            )
