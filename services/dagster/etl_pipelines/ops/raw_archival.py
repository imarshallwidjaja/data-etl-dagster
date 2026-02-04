# =============================================================================
# Raw Archival Helper - Hash-first blob storage
# =============================================================================
# Streams raw source bytes from S3, computes SHA256 while writing, and archives
# into content-addressed blob storage with per-upload artifact records.
# =============================================================================

import hashlib
import tempfile
from datetime import datetime, timezone
from pathlib import Path


from dagster import op, OpExecutionContext, In, Out

from libs.s3_utils import parse_s3_path


def _build_blob_key(digest: str) -> str:
    return f"blobs/sha256/{digest[:2]}/{digest}"


def _is_blob_path(bucket: str, key: str, lake_bucket: str) -> bool:
    return bucket == lake_bucket and key.startswith("blobs/sha256/")


def _derive_digest_from_blob_key(key: str) -> str:
    return key.split("/")[-1]


def _write_activity_log(
    *,
    mongodb,
    action: str,
    resource_type: str,
    resource_id: str,
    user: str,
    timestamp: datetime,
    details: dict[str, object],
) -> None:
    """
    Write an activity log entry with idempotent upsert.

    Uses (action, resource_type, resource_id) as the compound key for deduplication.
    """
    collection = mongodb._get_collection("activity_logs")
    collection.update_one(
        {
            "action": action,
            "resource_type": resource_type,
            "resource_id": resource_id,
        },
        {
            "$setOnInsert": {
                "timestamp": timestamp,
                "user": user,
                "action": action,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "details": details,
            }
        },
        upsert=True,
    )


def archive_raw_source(
    *,
    minio,
    mongodb,
    source_s3_path: str,
    batch_id: str,
    uploader: str,
    run_id: str | None,
    log,
) -> dict:
    """
    Archive a raw source file into content-addressed blob storage.

    Steps:
    1) Stream-download to temp file and compute sha256 as we write.
    2) Compute blob key from digest.
    3) Check Mongo for blob by content_hash, skip upload if exists.
    4) Insert blob document (handle duplicate races).
    5) Insert RAW_SOURCE artifact referencing the blob.
    """

    bucket, key = parse_s3_path(source_s3_path)
    now = datetime.now(timezone.utc)
    original_filename = Path(key).name

    if _is_blob_path(bucket, key, minio.lake_bucket):
        digest = _derive_digest_from_blob_key(key)
        content_hash = f"sha256:{digest}"
        blob_doc = mongodb.get_blob_by_hash(content_hash)

        if not blob_doc:
            size_bytes = None
            content_type = None
            try:
                stat = minio.stat_object(bucket, key)
                size_bytes = getattr(stat, "size", None)
                content_type = getattr(stat, "content_type", None)
            except Exception as exc:
                log.warning(f"Failed to stat blob {bucket}/{key}: {exc}")

            blob_doc = _insert_blob_with_race_handling(
                mongodb=mongodb,
                content_hash=content_hash,
                bucket=bucket,
                key=key,
                size_bytes=size_bytes,
                content_type=content_type,
                created_at=now,
            )

        blob_id = blob_doc["id"]
    else:
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_path = temp_file.name
        temp_file.close()

        response = None
        try:
            client = minio.get_client()
            response = client.get_object(bucket, key)

            sha256 = hashlib.sha256()
            with open(temp_path, "wb") as output:
                for chunk in response.stream(32 * 1024):
                    sha256.update(chunk)
                    output.write(chunk)

            digest = sha256.hexdigest()
            content_hash = f"sha256:{digest}"
            blob_key = _build_blob_key(digest)

            blob_doc = mongodb.get_blob_by_hash(content_hash)
            if blob_doc:
                blob_id = blob_doc["id"]
            else:
                minio.upload_to_lake(temp_path, blob_key)

                blob_doc = _insert_blob_with_race_handling(
                    mongodb=mongodb,
                    content_hash=content_hash,
                    bucket=minio.lake_bucket,
                    key=blob_key,
                    size_bytes=Path(temp_path).stat().st_size,
                    content_type=None,
                    created_at=now,
                )
                blob_id = blob_doc["id"]
        finally:
            if response is not None:
                try:
                    response.close()
                    response.release_conn()
                except Exception as exc:
                    log.warning(f"Failed to close response for {bucket}/{key}: {exc}")

            try:
                Path(temp_path).unlink(missing_ok=True)
            except Exception as exc:
                log.warning(f"Failed to cleanup temp file {temp_path}: {exc}")

    artifact = {
        "kind": "raw_source",
        "blob_id": blob_id,
        "batch_id": batch_id,
        "run_id": run_id,
        "created_at": now,
        "source_s3_path": source_s3_path,
        "original_filename": original_filename,
        "uploader": uploader,
        "content_type": None,
    }

    artifact_id = mongodb.insert_artifact(artifact)

    _write_activity_log(
        mongodb=mongodb,
        action="archive_raw_source",
        resource_type="artifact",
        resource_id=artifact_id,
        user=uploader,
        timestamp=now,
        details={
            "artifact_id": artifact_id,
            "blob_id": blob_id,
            "batch_id": batch_id,
            "content_hash": content_hash,
            "source_s3_path": source_s3_path,
            "blob_key": blob_doc["key"],
        },
    )

    return {
        "artifact_id": artifact_id,
        "blob_id": blob_id,
        "content_hash": content_hash,
        "blob_s3_path": f"s3://{blob_doc['bucket']}/{blob_doc['key']}",
    }


def _insert_blob_with_race_handling(
    *,
    mongodb,
    content_hash: str,
    bucket: str,
    key: str,
    size_bytes: int | None,
    content_type: str | None,
    created_at: datetime,
) -> dict:
    blob = {
        "content_hash": content_hash,
        "bucket": bucket,
        "key": key,
        "size_bytes": size_bytes,
        "content_type": content_type,
        "created_at": created_at,
    }

    blob_id = mongodb.insert_blob(blob)
    return {**blob, "id": blob_id}


@op(
    ins={"manifest": In(dagster_type=dict)},
    out={"manifest": Out(dagster_type=dict)},
    required_resource_keys={"minio", "mongodb"},
)
def archive_raw_sources_op(context: OpExecutionContext, manifest: dict) -> dict:
    """
    Archive raw source files listed in the manifest.

    Returns the manifest unchanged for downstream ops.
    """
    uploader = manifest.get("uploader", "unknown")
    batch_id = manifest.get("batch_id")
    if not batch_id:
        raise ValueError("Manifest is missing batch_id for raw archival")
    run_id = context.resources.mongodb.get_run_object_id(context.run_id)

    for file_entry in manifest.get("files", []):
        source_path = file_entry.get("path")
        if not source_path:
            continue
        archive_raw_source(
            minio=context.resources.minio,
            mongodb=context.resources.mongodb,
            source_s3_path=source_path,
            batch_id=batch_id,
            uploader=uploader,
            run_id=run_id,
            log=context.log,
        )

    return manifest
