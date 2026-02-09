"""Integration test: End-to-end complex spreadsheet pipeline via Dagster GraphQL.

This test validates the complex spreadsheet ETL loop:
1. Programmatically create a tiny XLSX fixture (two sheets, one with anchor)
2. Upload XLSX + parent manifest (intent=ingest_complex_spreadsheet) to landing-zone
3. Launch complex_table_splitter_job via GraphQL (op-based)
4. Verify splitter job completes, child manifests published, intermediate artifacts exist
5. Launch tabular_asset_job for a child manifest, verify Parquet in data-lake + Asset in MongoDB
6. Clean up ALL artifacts (MinIO, MongoDB, dynamic partitions)

Run with:
    uv run python scripts/worktree_stack.py test -- -m integration \
        tests/integration/test_complex_spreadsheet_e2e.py -q
"""

from __future__ import annotations

import json
from io import BytesIO
from uuid import uuid4

import pytest
from openpyxl import Workbook

from .helpers import (
    DagsterGraphQLClient,
    add_dynamic_partition,
    assert_datalake_object_exists,
    assert_parquet_valid,
    build_test_run_tags,
    cleanup_dynamic_partitions,
    cleanup_minio_manifest,
    cleanup_minio_object,
    cleanup_mongodb_activity_logs,
    cleanup_mongodb_manifest,
    cleanup_mongodb_run,
    format_error_details,
    poll_run_to_completion,
    upload_bytes_to_minio,
)


pytestmark = [pytest.mark.integration, pytest.mark.e2e]


# =============================================================================
# XLSX Fixture Generation
# =============================================================================


def _create_test_xlsx() -> bytes:
    """Build a tiny XLSX in memory with two sheets.

    Sheet A ("Data Sheet"):
      - Row 1: title/junk row (no anchor)
      - Row 2: header row containing anchor "Year" and value column headers
      - Rows 3-5: data rows

    Sheet B ("Notes"):
      - Contains text but NO anchor string "Year" => splitter will skip it.
    """
    wb = Workbook()

    # Sheet A - has anchor "Year"
    ws_a = wb.active
    ws_a.title = "Data Sheet"
    ws_a.append(["Region Statistics", None, None, None])  # row 1 - junk
    ws_a.append(["Year", "Metric A", "Metric B", "Metric C"])  # row 2 - anchor
    ws_a.append([2020, 10, 20, 30])
    ws_a.append([2021, 11, 21, 31])
    ws_a.append([2022, 12, 22, 32])

    # Sheet B - no anchor
    ws_b = wb.create_sheet("Notes")
    ws_b.append(["This sheet has no anchor"])
    ws_b.append(["Just some notes"])

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


# =============================================================================
# Parent Manifest Builder
# =============================================================================


def _build_parent_manifest(
    batch_id: str,
    xlsx_s3_path: str,
    dataset_id: str,
) -> dict:
    """Build a valid parent manifest for ingest_complex_spreadsheet."""
    return {
        "batch_id": batch_id,
        "uploader": "integration-test",
        "intent": "ingest_complex_spreadsheet",
        "files": [
            {
                "path": xlsx_s3_path,
                "type": "tabular",
                "format": "XLSX",
            }
        ],
        "metadata": {
            "title": "E2E Complex Spreadsheet Test",
            "description": "Integration test fixture",
            "keywords": ["e2e", "complex_spreadsheet"],
            "source": "integration-test",
            "license": "MIT",
            "attribution": "Test Suite",
            "project": "E2E_TEST",
            "tags": {
                "dataset_id": dataset_id,
                "testing": True,
            },
            "complex_spreadsheet": {
                "template_id": "anchor_unpivot_v1",
                "template_params": {"anchor_text": "Year"},
            },
        },
    }


# =============================================================================
# GraphQL Launchers
# =============================================================================

_LAUNCH_MUTATION = """
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


def _launch_splitter_job(
    client: DagsterGraphQLClient,
    manifest: dict,
) -> str:
    """Launch complex_table_splitter_job (op-based) via GraphQL."""
    variables = {
        "repositoryLocationName": "etl_pipelines",
        "repositoryName": "__repository__",
        "jobName": "complex_table_splitter_job",
        "runConfigData": {
            "ops": {
                "init_mongo_run_op": {
                    "inputs": {
                        "payload": {
                            "value": manifest,
                        }
                    }
                }
            }
        },
        "executionMetadata": {
            "tags": build_test_run_tags(
                batch_id=manifest["batch_id"],
                test_run_id=manifest["batch_id"],
            )
        },
    }

    result = client.query(_LAUNCH_MUTATION, variables=variables, timeout=10)
    assert "errors" not in result, (
        f"Failed to launch splitter job: {result.get('errors')}"
    )

    launch_data = result["data"]["launchRun"]
    assert "run" in launch_data, (
        f"Splitter job launch failed: {launch_data.get('message', 'Unknown error')}"
    )

    run_id = launch_data["run"]["runId"]
    assert run_id, "No run_id returned from splitter job launch"
    return run_id


def _launch_tabular_asset_job(
    client: DagsterGraphQLClient,
    manifest: dict,
) -> str:
    """Launch tabular_asset_job (asset-based) via GraphQL."""
    partition_key = manifest["metadata"]["tags"]["dataset_id"]
    variables = {
        "repositoryLocationName": "etl_pipelines",
        "repositoryName": "__repository__",
        "jobName": "tabular_asset_job",
        "runConfigData": {
            "ops": {"raw_manifest_json": {"config": {"manifest": manifest}}}
        },
        "executionMetadata": {
            "tags": build_test_run_tags(
                partition_key=partition_key,
                batch_id=manifest["batch_id"],
                test_run_id=manifest["batch_id"],
            )
        },
    }

    result = client.query(_LAUNCH_MUTATION, variables=variables, timeout=10)
    assert "errors" not in result, (
        f"Failed to launch tabular job: {result.get('errors')}"
    )

    launch_data = result["data"]["launchRun"]
    assert "run" in launch_data, (
        f"Tabular job launch failed: {launch_data.get('message', 'Unknown error')}"
    )

    run_id = launch_data["run"]["runId"]
    assert run_id, "No run_id returned from tabular job launch"
    return run_id


# =============================================================================
# MongoDB / MinIO Assertion Helpers
# =============================================================================


def _assert_intermediate_artifacts_exist(
    mongo_client, mongo_settings, batch_id: str
) -> list[dict]:
    """Assert intermediate artifacts were registered for the splitter batch."""
    db = mongo_client[mongo_settings.database]
    artifacts = list(
        db["artifacts"].find({"batch_id": batch_id, "kind": "intermediate"})
    )
    assert artifacts, f"No intermediate artifacts found for batch_id={batch_id}"
    return artifacts


def _assert_raw_archives_exist(
    mongo_client, mongo_settings, batch_id: str
) -> list[dict]:
    """Assert raw_source artifacts were registered."""
    db = mongo_client[mongo_settings.database]
    artifacts = list(db["artifacts"].find({"batch_id": batch_id, "kind": "raw_source"}))
    assert artifacts, f"No raw_source artifacts found for batch_id={batch_id}"
    return artifacts


def _assert_mongodb_asset_exists(
    mongo_client, mongo_settings, dagster_run_id: str
) -> dict:
    """Assert asset record exists via canonical run -> asset linkage."""
    db = mongo_client[mongo_settings.database]
    run_doc = db["runs"].find_one({"dagster_run_id": dagster_run_id})
    assert run_doc is not None, (
        f"No run document found for dagster_run_id: {dagster_run_id}"
    )
    mongodb_run_id = str(run_doc["_id"])
    asset_doc = db["assets"].find_one({"run_id": mongodb_run_id})
    assert asset_doc is not None, (
        f"No asset record found for run_id: {mongodb_run_id} "
        f"(Dagster run: {dagster_run_id})"
    )
    return asset_doc


def _get_child_manifest_from_landing_or_archive(
    minio_client,
    minio_settings,
    child_manifest_key: str,
) -> dict | None:
    """Try to read a child manifest from landing zone or archive.

    After the splitter publishes, the tabular sensor may have already
    archived it. Check both locations.
    """
    from minio.error import S3Error

    bucket = minio_settings.landing_bucket

    for key in [child_manifest_key, f"archive/{child_manifest_key}"]:
        try:
            response = minio_client.get_object(bucket, key)
            data = json.loads(response.read())
            response.close()
            response.release_conn()
            return data
        except S3Error:
            continue
    return None


# =============================================================================
# Cleanup
# =============================================================================


def _cleanup_all(
    minio_client,
    minio_settings,
    mongo_client,
    mongo_settings,
    dagster_client,
    *,
    xlsx_key: str,
    parent_batch_id: str,
    child_batch_ids: list[str],
    splitter_run_id: str | None,
    tabular_run_ids: list[str],
    asset_docs: list[dict],
    created_partitions: set[str],
) -> None:
    """Best-effort cleanup of all test artifacts."""
    # MinIO: XLSX file
    cleanup_minio_object(minio_client, minio_settings.landing_bucket, xlsx_key)

    # MinIO: parent manifest (landing + archive)
    parent_manifest_key = f"manifests/{parent_batch_id}.json"
    cleanup_minio_manifest(minio_client, minio_settings, parent_manifest_key)

    # MinIO: child manifests (landing + archive)
    for child_batch_id in child_batch_ids:
        child_key = f"manifests/{child_batch_id}.json"
        cleanup_minio_manifest(minio_client, minio_settings, child_key)

    # MongoDB: parent manifest
    cleanup_mongodb_manifest(mongo_client, mongo_settings, parent_batch_id)

    # MongoDB: child manifests
    for child_batch_id in child_batch_ids:
        cleanup_mongodb_manifest(mongo_client, mongo_settings, child_batch_id)

    # MongoDB: splitter run
    if splitter_run_id:
        cleanup_mongodb_activity_logs(mongo_client, mongo_settings, splitter_run_id)
        cleanup_mongodb_run(mongo_client, mongo_settings, splitter_run_id)

    # MongoDB: tabular runs
    for run_id in tabular_run_ids:
        cleanup_mongodb_activity_logs(mongo_client, mongo_settings, run_id)
        cleanup_mongodb_run(mongo_client, mongo_settings, run_id)

    # MongoDB: artifacts (parent batch + child batches)
    db = mongo_client[mongo_settings.database]
    try:
        db["artifacts"].delete_many({"batch_id": parent_batch_id})
    except Exception:
        pass
    for child_batch_id in child_batch_ids:
        try:
            db["artifacts"].delete_many({"batch_id": child_batch_id})
        except Exception:
            pass

    # MongoDB + MinIO: asset docs and their data-lake objects
    for asset_doc in asset_docs:
        s3_key = asset_doc.get("s3_key", "")
        if s3_key:
            cleanup_minio_object(minio_client, minio_settings.lake_bucket, s3_key)
        try:
            db["assets"].delete_one({"_id": asset_doc["_id"]})
        except Exception:
            pass

    # MinIO: blob objects from intermediate artifacts
    try:
        all_artifacts = list(
            db["artifacts"].find(
                {"batch_id": parent_batch_id, "kind": "intermediate"},
                {"blob_s3_path": 1, "blob_id": 1},
            )
        )
        for art in all_artifacts:
            blob_path = art.get("blob_s3_path", "")
            if blob_path.startswith("s3://"):
                # Parse bucket/key from s3://bucket/key
                parts = blob_path[5:].split("/", 1)
                if len(parts) == 2:
                    cleanup_minio_object(minio_client, parts[0], parts[1])
    except Exception:
        pass

    # MongoDB: blobs for this batch's artifacts
    try:
        blob_ids = set()
        for art in db["artifacts"].find({"batch_id": parent_batch_id}, {"blob_id": 1}):
            if art.get("blob_id"):
                blob_ids.add(art["blob_id"])
        for bid in blob_ids:
            try:
                from bson import ObjectId

                db["blobs"].delete_one({"_id": ObjectId(bid)})
            except Exception:
                pass
    except Exception:
        pass

    # Dynamic partitions
    cleanup_dynamic_partitions(dagster_client, created_partitions)


# =============================================================================
# Tests
# =============================================================================


class TestComplexSpreadsheetE2E:
    """End-to-end test: XLSX -> splitter -> child manifests -> tabular assets."""

    def test_complex_spreadsheet_full_pipeline(
        self,
        dagster_client,
        minio_client,
        minio_settings,
        mongo_client,
        mongo_settings,
    ):
        """Splitter splits XLSX, produces child manifest, tabular job produces asset."""
        uid = uuid4().hex[:12]
        parent_batch_id = f"e2e_complex_{uid}"
        dataset_id = f"complex_dataset_{uid}"
        xlsx_key = f"e2e/{parent_batch_id}/spreadsheet.xlsx"

        # Expected child: only "Data Sheet" has anchor "Year"
        safe_sheet = "data_sheet"  # "Data Sheet" -> "data_sheet"
        child_batch_id = f"{parent_batch_id}__{safe_sheet}"
        child_dataset_id = f"{dataset_id}__{safe_sheet}"
        child_batch_ids = [child_batch_id]

        splitter_run_id: str | None = None
        tabular_run_ids: list[str] = []
        asset_docs: list[dict] = []
        created_partitions: set[str] = set()

        try:
            # --- Step 1: Create and upload XLSX ---
            xlsx_bytes = _create_test_xlsx()
            upload_bytes_to_minio(
                minio_client,
                minio_settings.landing_bucket,
                xlsx_key,
                xlsx_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            # --- Step 2: Build and launch splitter job ---
            parent_manifest = _build_parent_manifest(
                batch_id=parent_batch_id,
                xlsx_s3_path=f"s3://landing-zone/{xlsx_key}",
                dataset_id=dataset_id,
            )

            splitter_run_id = _launch_splitter_job(dagster_client, parent_manifest)

            status, error_details = poll_run_to_completion(
                dagster_client, splitter_run_id
            )
            if status != "SUCCESS":
                pytest.fail(
                    f"complex_table_splitter_job failed: {status}."
                    f"{format_error_details(error_details)}"
                )

            # --- Step 3: Assert splitter outputs ---

            # 3a. Raw source artifacts archived
            _assert_raw_archives_exist(mongo_client, mongo_settings, parent_batch_id)

            # 3b. Intermediate artifacts registered
            intermediates = _assert_intermediate_artifacts_exist(
                mongo_client, mongo_settings, parent_batch_id
            )
            assert len(intermediates) == 1, (
                f"Expected 1 intermediate artifact (one sheet with anchor), "
                f"got {len(intermediates)}"
            )

            # 3c. Child manifest published (in landing or archive)
            child_manifest_key = f"manifests/{child_batch_id}.json"
            child_manifest = _get_child_manifest_from_landing_or_archive(
                minio_client, minio_settings, child_manifest_key
            )
            assert child_manifest is not None, (
                f"Child manifest not found at {child_manifest_key} "
                "(checked landing + archive)"
            )
            assert child_manifest["intent"] == "ingest_tabular"
            assert child_manifest["batch_id"] == child_batch_id
            assert child_manifest["files"][0]["format"] == "Parquet"
            assert child_manifest["metadata"]["tags"]["dataset_id"] == child_dataset_id
            assert (
                child_manifest["metadata"]["tags"]["parent_batch_id"] == parent_batch_id
            )

            # --- Step 4: Launch tabular_asset_job for child manifest ---
            child_partition_key = child_dataset_id
            add_dynamic_partition(dagster_client, child_partition_key)
            created_partitions.add(child_partition_key)

            tabular_run_id = _launch_tabular_asset_job(dagster_client, child_manifest)
            tabular_run_ids.append(tabular_run_id)

            status, error_details = poll_run_to_completion(
                dagster_client, tabular_run_id
            )
            if status != "SUCCESS":
                pytest.fail(
                    f"tabular_asset_job failed for child: {status}."
                    f"{format_error_details(error_details)}"
                )

            # --- Step 5: Assert tabular outputs ---

            # 5a. Asset exists in MongoDB
            asset_doc = _assert_mongodb_asset_exists(
                mongo_client, mongo_settings, tabular_run_id
            )
            asset_docs.append(asset_doc)

            assert asset_doc.get("kind") == "tabular"
            assert asset_doc.get("format") == "parquet"

            # 5b. Parquet exists in data-lake
            s3_key = asset_doc.get("s3_key")
            assert s3_key, "Asset document missing s3_key"
            assert_datalake_object_exists(
                minio_client, minio_settings.lake_bucket, s3_key
            )

            # 5c. Parquet content valid (melted long format: Year, variable, value)
            assert_parquet_valid(
                minio_client,
                minio_settings.lake_bucket,
                s3_key,
                expected_columns=["year", "variable", "value"],
                min_rows=1,
            )

        except BaseException:
            raise

        finally:
            _cleanup_all(
                minio_client,
                minio_settings,
                mongo_client,
                mongo_settings,
                dagster_client,
                xlsx_key=xlsx_key,
                parent_batch_id=parent_batch_id,
                child_batch_ids=child_batch_ids,
                splitter_run_id=splitter_run_id,
                tabular_run_ids=tabular_run_ids,
                asset_docs=asset_docs,
                created_partitions=created_partitions,
            )
