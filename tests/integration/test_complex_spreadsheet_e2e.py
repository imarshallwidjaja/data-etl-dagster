"""Integration smoke test: XLSX -> splitter -> child manifests -> tabular assets."""

from __future__ import annotations

import json
import time
from io import BytesIO
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest
from dagster import DagsterInstance, RunRequest
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient

from libs.models import MinIOSettings, MongoSettings
from services.dagster.etl_pipelines.resources import MinIOResource
from services.dagster.etl_pipelines.sensors.complex_spreadsheet_sensor import (
    complex_spreadsheet_sensor,
)

from .helpers import (
    DagsterGraphQLClient,
    assert_datalake_object_exists,
    assert_mongodb_asset_exists,
    assert_parquet_valid,
    build_test_run_tags,
    cleanup_dynamic_partitions,
    cleanup_minio_object,
    cleanup_mongodb_activity_logs,
    cleanup_mongodb_asset,
    cleanup_mongodb_manifest,
    cleanup_mongodb_run,
    format_error_details,
    poll_run_to_completion,
)


pytestmark = [pytest.mark.integration, pytest.mark.e2e]

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "complex_spreadsheet"
XLSX_FIXTURE_PATH = FIXTURES_DIR / "simple_anchor_unpivot.xlsx"


@pytest.fixture
def dagster_instance() -> DagsterInstance:
    return DagsterInstance.ephemeral()


@pytest.fixture
def minio_resource(minio_settings: MinIOSettings) -> MinIOResource:
    return MinIOResource(
        endpoint=minio_settings.endpoint,
        access_key=minio_settings.access_key,
        secret_key=minio_settings.secret_key,
        use_ssl=minio_settings.use_ssl,
        landing_bucket=minio_settings.landing_bucket,
        lake_bucket=minio_settings.lake_bucket,
    )


def _load_xlsx_fixture_bytes() -> bytes:
    return XLSX_FIXTURE_PATH.read_bytes()


def _build_parent_manifest(*, batch_id: str, dataset_id: str, source_key: str) -> dict:
    return {
        "batch_id": batch_id,
        "uploader": "integration_test",
        "intent": "ingest_complex_spreadsheet",
        "files": [
            {
                "path": f"s3://landing-zone/{source_key}",
                "type": "tabular",
                "format": "XLSX",
            }
        ],
        "metadata": {
            "title": "Complex Spreadsheet Smoke Test",
            "description": "Integration smoke test for complex spreadsheet splitter",
            "keywords": ["integration", "complex_spreadsheet", "xlsx"],
            "source": "integration-test",
            "license": "MIT",
            "attribution": "integration-test-suite",
            "project": "E2E",
            "tags": {
                "dataset_id": dataset_id,
                "source": "integration-test",
                "testing": True,
            },
            "complex_spreadsheet": {
                "template_id": "anchor_unpivot_v1",
                "template_params": {
                    "anchor_text": "Region",
                    "anchor_match": "exact",
                    "header_rows": 2,
                    "id_column_count": 1,
                },
            },
        },
    }


def _upload_bytes(
    minio_client: Minio,
    bucket: str,
    object_key: str,
    data_bytes: bytes,
    content_type: str,
) -> None:
    minio_client.put_object(
        bucket,
        object_key,
        BytesIO(data_bytes),
        length=len(data_bytes),
        content_type=content_type,
    )


def _upload_manifest(
    minio_client: Minio,
    bucket: str,
    manifest_key: str,
    manifest: dict,
) -> None:
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
    _upload_bytes(
        minio_client, bucket, manifest_key, manifest_bytes, "application/json"
    )


def _evaluate_sensor(
    sensor_fn,
    dagster_instance: DagsterInstance,
    minio_resource: MinIOResource,
    expected_manifest_key: str,
) -> RunRequest:
    from unittest.mock import Mock

    context = Mock()
    context.instance = dagster_instance
    context.cursor = None
    context.log = Mock()
    context.log.info = Mock()
    context.log.error = Mock()
    context.log.warning = Mock()
    context.update_cursor = Mock()

    results = list(sensor_fn._raw_fn(context, minio_resource))
    run_requests = [
        result
        for result in results
        if isinstance(result, RunRequest)
        and result.tags.get("manifest_key") == expected_manifest_key
    ]
    assert len(run_requests) == 1, (
        f"Expected 1 RunRequest for manifest {expected_manifest_key}, got {len(run_requests)}"
    )
    return run_requests[0]


def _launch_job_from_run_request(
    dagster_client: DagsterGraphQLClient,
    *,
    job_name: str,
    run_request: RunRequest,
    test_run_id: str,
) -> str:
    mutation = """
    mutation LaunchRun($executionParams: ExecutionParams!) {
        launchRun(executionParams: $executionParams) {
            __typename
            ... on LaunchRunSuccess {
                run {
                    runId
                }
            }
            ... on PythonError {
                message
                stack
            }
            ... on InvalidSubsetError {
                message
            }
            ... on InvalidOutputError {
                stepKey
                invalidOutputName
            }
            ... on RunConfigValidationInvalid {
                errors {
                    message
                }
            }
        }
    }
    """

    tags = build_test_run_tags(
        partition_key=run_request.partition_key,
        batch_id=run_request.tags.get("batch_id"),
        manifest_key=run_request.tags.get("manifest_key"),
        test_run_id=test_run_id,
    )
    existing_keys = {tag["key"] for tag in tags}
    for key, value in run_request.tags.items():
        if key in existing_keys:
            continue
        tags.append({"key": key, "value": str(value)})

    run_config_data = cast(dict[str, object], run_request.run_config)
    if job_name == "complex_table_splitter_job":
        sensor_ops = run_request.run_config.get("ops")
        if not isinstance(sensor_ops, dict):
            raise RuntimeError(
                "Expected sensor run_config.ops for complex_table_splitter_job"
            )
        raw_manifest_json = sensor_ops.get("raw_manifest_json")
        if not isinstance(raw_manifest_json, dict):
            raise RuntimeError(
                "Expected run_config.ops.raw_manifest_json for complex_table_splitter_job"
            )
        raw_manifest_config = raw_manifest_json.get("config")
        if not isinstance(raw_manifest_config, dict):
            raise RuntimeError(
                "Expected run_config.ops.raw_manifest_json.config for complex_table_splitter_job"
            )
        manifest = raw_manifest_config.get("manifest")
        if not isinstance(manifest, dict):
            raise RuntimeError(
                "Expected manifest dict in run_config.ops.raw_manifest_json.config"
            )

        # Op-based splitter job starts at init_mongo_run_op(payload).
        run_config_data = {
            "ops": {
                "init_mongo_run_op": {
                    "inputs": {
                        "payload": {
                            "value": manifest,
                        }
                    }
                }
            }
        }

    variables = {
        "executionParams": {
            "selector": {
                "repositoryLocationName": "etl_pipelines",
                "repositoryName": "__repository__",
                "jobName": job_name,
            },
            "runConfigData": run_config_data,
            "executionMetadata": {"tags": tags},
        }
    }

    result = dagster_client.query(mutation, variables)

    if "errors" in result:
        raise RuntimeError(f"GraphQL errors: {result['errors']}")

    data = result.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected response: {result}")
    data_dict = cast(dict[str, object], data)

    launch_result = data_dict.get("launchRun")
    if not isinstance(launch_result, dict):
        raise RuntimeError(f"Unexpected launch result: {data}")
    launch_result_dict = cast(dict[str, object], launch_result)

    if launch_result_dict.get("__typename") != "LaunchRunSuccess":
        if launch_result_dict.get("__typename") == "RunConfigValidationInvalid":
            validation_errors = launch_result_dict.get("errors")
            raise RuntimeError(
                f"Launch failed with RunConfigValidationInvalid: {validation_errors}"
            )
        raise RuntimeError(f"Launch failed: {launch_result}")

    run = launch_result_dict.get("run")
    if not isinstance(run, dict):
        raise RuntimeError(f"Missing run in result: {launch_result}")
    run_dict = cast(dict[str, object], run)

    run_id = run_dict.get("runId")
    if not isinstance(run_id, str):
        raise RuntimeError(f"Missing runId: {run}")

    return run_id


def _assert_minio_object_exists(minio_client: Minio, bucket: str, key: str) -> None:
    try:
        _ = minio_client.stat_object(bucket, key)
    except S3Error as exc:
        raise AssertionError(f"Expected object to exist: {bucket}/{key}: {exc}")


def _assert_minio_object_not_exists(minio_client: Minio, bucket: str, key: str) -> None:
    try:
        _ = minio_client.stat_object(bucket, key)
        raise AssertionError(f"Expected object to NOT exist: {bucket}/{key}")
    except S3Error as exc:
        if exc.code != "NoSuchKey":
            raise


def _minio_object_exists(minio_client: Minio, bucket: str, key: str) -> bool:
    try:
        _ = minio_client.stat_object(bucket, key)
        return True
    except S3Error as exc:
        if exc.code == "NoSuchKey":
            return False
        raise


def _wait_for_minio_object(
    minio_client: Minio,
    bucket: str,
    key: str,
    *,
    timeout_seconds: int = 180,
    poll_seconds: float = 1.0,
) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if _minio_object_exists(minio_client, bucket, key):
            return
        time.sleep(poll_seconds)

    raise AssertionError(f"Timed out waiting for MinIO object: {bucket}/{key}")


def _read_json_object(minio_client: Minio, bucket: str, key: str) -> dict:
    response = minio_client.get_object(bucket, key)
    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


def _wait_for_run_id_by_batch(
    mongo_client: MongoClient,
    mongo_settings: MongoSettings,
    *,
    batch_id: str,
    job_name: str,
    timeout_seconds: int = 240,
    poll_seconds: float = 2.0,
) -> str:
    deadline = time.time() + timeout_seconds
    db = mongo_client[mongo_settings.database]

    while time.time() < deadline:
        run_doc = db["runs"].find_one(
            {"batch_id": batch_id, "job_name": job_name},
            sort=[("started_at", -1)],
        )
        if isinstance(run_doc, dict):
            dagster_run_id = run_doc.get("dagster_run_id")
            if isinstance(dagster_run_id, str) and dagster_run_id:
                return dagster_run_id
        time.sleep(poll_seconds)

    raise AssertionError(
        f"Timed out waiting for run (job={job_name}, batch_id={batch_id}) in MongoDB"
    )


def _cleanup_minio(
    minio_client: Minio,
    minio_settings: MinIOSettings,
    *,
    source_key: str,
    parent_manifest_key: str,
    child_manifest_key: str,
    skipped_child_manifest_key: str,
    asset_docs: list[dict],
) -> None:
    cleanup_minio_object(minio_client, minio_settings.landing_bucket, source_key)

    for key in [
        parent_manifest_key,
        child_manifest_key,
        skipped_child_manifest_key,
        f"archive/{parent_manifest_key}",
        f"archive/{child_manifest_key}",
        f"archive/{skipped_child_manifest_key}",
    ]:
        cleanup_minio_object(minio_client, minio_settings.landing_bucket, key)

    for asset_doc in asset_docs:
        s3_key = asset_doc.get("s3_key")
        if isinstance(s3_key, str) and s3_key:
            cleanup_minio_object(minio_client, minio_settings.lake_bucket, s3_key)


class TestComplexSpreadsheetE2E:
    def test_xlsx_splitter_child_manifests_and_tabular_assets(
        self,
        dagster_client,
        dagster_instance,
        minio_client,
        minio_settings,
        minio_resource,
        mongo_client,
        mongo_settings,
    ):
        test_uuid = uuid4().hex[:8]

        parent_batch_id = f"sensor_e2e_complex_{test_uuid}"
        parent_dataset_id = f"complex_dataset_{test_uuid}"

        source_key = f"e2e/{parent_batch_id}/simple_anchor_unpivot.xlsx"
        parent_manifest_key = f"manifests/{parent_batch_id}.json"

        child_sheet_slug = "sheet_a"
        skipped_sheet_slug = "sheet_b"

        child_batch_id = f"{parent_batch_id}__{child_sheet_slug}"
        child_dataset_id = f"{parent_dataset_id}__{child_sheet_slug}"
        child_manifest_key = f"manifests/{child_batch_id}.json"
        skipped_child_manifest_key = (
            f"manifests/{parent_batch_id}__{skipped_sheet_slug}.json"
        )

        splitter_run_id: str | None = None
        tabular_run_id: str | None = None
        child_asset_doc: dict | None = None
        created_partitions: set[str] = set()
        test_error: BaseException | None = None

        try:
            xlsx_bytes = _load_xlsx_fixture_bytes()
            parent_manifest = _build_parent_manifest(
                batch_id=parent_batch_id,
                dataset_id=parent_dataset_id,
                source_key=source_key,
            )

            _upload_bytes(
                minio_client,
                minio_settings.landing_bucket,
                source_key,
                xlsx_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            _upload_manifest(
                minio_client,
                minio_settings.landing_bucket,
                parent_manifest_key,
                parent_manifest,
            )

            splitter_run_request = _evaluate_sensor(
                complex_spreadsheet_sensor,
                dagster_instance,
                minio_resource,
                parent_manifest_key,
            )
            created_partitions.add(parent_dataset_id)

            assert splitter_run_request.partition_key == parent_dataset_id
            assert splitter_run_request.tags.get("testing") == "true"

            _assert_minio_object_exists(
                minio_client,
                minio_settings.landing_bucket,
                f"archive/{parent_manifest_key}",
            )

            splitter_run_id = _launch_job_from_run_request(
                dagster_client,
                job_name="complex_table_splitter_job",
                run_request=splitter_run_request,
                test_run_id=f"{test_uuid}-split",
            )

            status, error_details = poll_run_to_completion(
                dagster_client, splitter_run_id
            )
            if status != "SUCCESS":
                pytest.fail(
                    f"complex_table_splitter_job failed: {status}.{format_error_details(error_details)}"
                )

            _wait_for_minio_object(
                minio_client,
                minio_settings.landing_bucket,
                f"archive/{child_manifest_key}",
            )
            _assert_minio_object_not_exists(
                minio_client,
                minio_settings.landing_bucket,
                skipped_child_manifest_key,
            )
            _assert_minio_object_not_exists(
                minio_client,
                minio_settings.landing_bucket,
                f"archive/{skipped_child_manifest_key}",
            )

            child_manifest = _read_json_object(
                minio_client,
                minio_settings.landing_bucket,
                f"archive/{child_manifest_key}",
            )
            assert child_manifest["batch_id"] == child_batch_id
            assert child_manifest["intent"] == "ingest_tabular"
            assert child_manifest["files"][0]["format"] == "Parquet"
            assert child_manifest["files"][0]["type"] == "tabular"
            assert child_manifest["metadata"]["tags"]["dataset_id"] == child_dataset_id
            assert (
                child_manifest["metadata"]["tags"]["parent_batch_id"] == parent_batch_id
            )
            assert child_manifest["metadata"]["tags"]["source_sheet"] == "Sheet A"

            child_blob_path = child_manifest["files"][0]["path"]
            assert child_blob_path.startswith(
                f"s3://{minio_settings.lake_bucket}/blobs/sha256/"
            )

            db = mongo_client[mongo_settings.database]
            raw_artifacts = list(
                db["artifacts"].find(
                    {"batch_id": parent_batch_id, "kind": "raw_source"}
                )
            )
            assert raw_artifacts, (
                f"Expected raw_source artifacts for batch {parent_batch_id}"
            )

            intermediate_artifacts = list(
                db["artifacts"].find(
                    {"batch_id": parent_batch_id, "kind": "intermediate"}
                )
            )
            assert len(intermediate_artifacts) == 1

            intermediate_blob_id = intermediate_artifacts[0].get("blob_id")
            assert isinstance(intermediate_blob_id, str) and intermediate_blob_id

            from bson import ObjectId

            intermediate_blob = db["blobs"].find_one(
                {"_id": ObjectId(intermediate_blob_id)}
            )
            assert intermediate_blob is not None
            assert (
                f"s3://{intermediate_blob['bucket']}/{intermediate_blob['key']}"
                == child_blob_path
            )

            # Child manifest is processed by daemonized tabular_sensor.
            created_partitions.add(child_dataset_id)
            tabular_run_id = _wait_for_run_id_by_batch(
                mongo_client,
                mongo_settings,
                batch_id=child_batch_id,
                job_name="tabular_asset_job",
            )

            status, error_details = poll_run_to_completion(
                dagster_client, tabular_run_id
            )
            if status != "SUCCESS":
                pytest.fail(
                    f"tabular_asset_job failed for child manifest: {status}.{format_error_details(error_details)}"
                )

            _assert_minio_object_exists(
                minio_client,
                minio_settings.landing_bucket,
                f"archive/{child_manifest_key}",
            )

            child_asset_doc = assert_mongodb_asset_exists(
                mongo_client,
                mongo_settings,
                tabular_run_id,
            )
            assert child_asset_doc.get("dataset_id") == child_dataset_id

            metadata = child_asset_doc.get("metadata") or {}
            tags = metadata.get("tags") or {}
            assert tags.get("parent_batch_id") == parent_batch_id
            assert tags.get("source_sheet") == "Sheet A"

            s3_key = child_asset_doc.get("s3_key")
            assert isinstance(s3_key, str) and s3_key
            assert_datalake_object_exists(
                minio_client,
                minio_settings.lake_bucket,
                s3_key,
            )
            assert_parquet_valid(
                minio_client,
                minio_settings.lake_bucket,
                s3_key,
                expected_columns=["region", "variable", "value"],
            )

        except BaseException as exc:
            test_error = exc
            raise
        finally:
            _cleanup_minio(
                minio_client,
                minio_settings,
                source_key=source_key,
                parent_manifest_key=parent_manifest_key,
                child_manifest_key=child_manifest_key,
                skipped_child_manifest_key=skipped_child_manifest_key,
                asset_docs=[doc for doc in [child_asset_doc] if doc is not None],
            )

            db = mongo_client[mongo_settings.database]
            db["artifacts"].delete_many(
                {"batch_id": {"$in": [parent_batch_id, child_batch_id]}}
            )

            for batch_id in [parent_batch_id, child_batch_id]:
                cleanup_mongodb_manifest(mongo_client, mongo_settings, batch_id)

            for run_id in [splitter_run_id, tabular_run_id]:
                if not run_id:
                    continue
                cleanup_mongodb_activity_logs(mongo_client, mongo_settings, run_id)
                cleanup_mongodb_run(mongo_client, mongo_settings, run_id)

            cleanup_mongodb_asset(mongo_client, mongo_settings, child_asset_doc)
            cleanup_dynamic_partitions(
                dagster_client,
                created_partitions,
                original_error=test_error,
            )
