# =============================================================================
# Webapp Manifests Rerun Integration Tests
# =============================================================================
# Tests rerun endpoint rewrites archived manifest paths to blob paths.
# =============================================================================

from __future__ import annotations

import json
from datetime import datetime, timezone
import hashlib
from uuid import uuid4

import pytest
import requests
from bson import ObjectId

from .helpers import cleanup_minio_manifest, upload_bytes_to_minio

AUTH = ("admin", "admin")


@pytest.mark.integration
def test_webapp_rerun_rewrites_manifest_paths_to_blobs(
    minio_client,
    minio_settings,
    mongo_client,
    mongo_settings,
    webapp_url,
):
    batch_id = f"test_rerun_{uuid4().hex[:8]}"
    manifest_key = f"manifests/{batch_id}.json"
    archive_key = f"archive/{manifest_key}"

    source_paths = [
        "s3://landing-zone/raw/file1.csv",
        "s3://landing-zone/raw/file2.csv",
    ]
    blob_keys = ["blobs/file1.parquet", "blobs/file2.parquet"]

    archived_manifest = {
        "batch_id": batch_id,
        "uploader": "integration_test",
        "intent": "ingest_tabular",
        "files": [
            {"path": source_paths[0], "type": "tabular", "format": "CSV"},
            {"path": source_paths[1], "type": "tabular", "format": "CSV"},
        ],
        "metadata": {"tags": {"testing": True}},
    }

    db = mongo_client[mongo_settings.database]
    blob_ids: list[ObjectId] = []
    artifact_ids: list[ObjectId] = []
    new_manifest_key: str | None = None

    try:
        upload_bytes_to_minio(
            minio_client,
            minio_settings.landing_bucket,
            archive_key,
            json.dumps(archived_manifest, indent=2).encode("utf-8"),
            "application/json",
        )

        for source_path, blob_key in zip(source_paths, blob_keys):
            digest = hashlib.sha256(
                f"{source_path}:{batch_id}".encode("utf-8")
            ).hexdigest()
            blob_id = ObjectId()
            blob_ids.append(blob_id)
            db["blobs"].insert_one(
                {
                    "_id": blob_id,
                    "content_hash": f"sha256:{digest}",
                    "bucket": minio_settings.lake_bucket,
                    "key": blob_key,
                    "created_at": datetime.now(timezone.utc),
                    "size_bytes": 123,
                    "content_type": "application/parquet",
                }
            )

            artifact_result = db["artifacts"].insert_one(
                {
                    "kind": "raw_source",
                    "blob_id": str(blob_id),
                    "batch_id": batch_id,
                    "run_id": None,
                    "created_at": datetime.now(timezone.utc),
                    "source_s3_path": source_path,
                    "original_filename": source_path.split("/")[-1],
                    "uploader": "integration_test",
                    "content_type": None,
                    "parameters": {},
                }
            )
            artifact_ids.append(artifact_result.inserted_id)

        response = requests.post(
            f"{webapp_url}/manifests/{batch_id}/rerun",
            auth=AUTH,
            timeout=10,
        )

        assert response.status_code == 200
        payload = response.json()
        new_batch_id = payload.get("new_batch_id")
        new_manifest_key = payload.get("manifest_key")

        assert isinstance(new_batch_id, str) and new_batch_id
        assert new_manifest_key == f"manifests/{new_batch_id}.json"

        manifest_response = minio_client.get_object(
            minio_settings.landing_bucket,
            new_manifest_key,
        )
        try:
            new_manifest = json.loads(manifest_response.read().decode("utf-8"))
        finally:
            manifest_response.close()
            manifest_response.release_conn()

        assert new_manifest["batch_id"] == new_batch_id
        assert [file_entry["path"] for file_entry in new_manifest["files"]] == [
            f"s3://{minio_settings.lake_bucket}/{blob_keys[0]}",
            f"s3://{minio_settings.lake_bucket}/{blob_keys[1]}",
        ]

    finally:
        if artifact_ids:
            db["artifacts"].delete_many({"_id": {"$in": artifact_ids}})
        if blob_ids:
            db["blobs"].delete_many({"_id": {"$in": blob_ids}})
        cleanup_minio_manifest(minio_client, minio_settings, manifest_key)
        if new_manifest_key:
            cleanup_minio_manifest(minio_client, minio_settings, new_manifest_key)
