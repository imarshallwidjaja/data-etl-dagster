# =============================================================================
# Intermediate Artifacts Helper - Hash-first blob storage
# =============================================================================
# Registers locally produced intermediate outputs into content-addressed blob
# storage with per-upload artifact records.
# =============================================================================

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from libs.models.artifact import Artifact


def _build_blob_key(digest: str) -> str:
    return f"blobs/sha256/{digest[:2]}/{digest}"


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


def register_intermediate_from_local_file(
    *,
    local_path: str,
    batch_id: str,
    run_id: str | None,
    producer: str,
    label: str,
    parameters: dict[str, Any] | None = None,
    content_type: str | None = None,
    minio,
    mongodb,
    log,
) -> dict:
    """
    Register a local intermediate file into content-addressed blob storage.

    Steps:
    1) Compute sha256 of local file.
    2) Compute blob key from digest.
    3) Check Mongo for blob by content_hash, skip upload if exists.
    4) Insert blob document (handle duplicate races).
    5) Insert intermediate artifact referencing the blob.
    """

    now = datetime.now(timezone.utc)
    path = Path(local_path)

    sha256 = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(32 * 1024), b""):
            sha256.update(chunk)

    digest = sha256.hexdigest()
    content_hash = f"sha256:{digest}"
    blob_key = _build_blob_key(digest)

    blob_doc = mongodb.get_blob_by_hash(content_hash)
    if blob_doc:
        blob_id = blob_doc["id"]
    else:
        minio.upload_to_lake(local_path, blob_key, content_type=content_type)

        blob_doc = _insert_blob_with_race_handling(
            mongodb=mongodb,
            content_hash=content_hash,
            bucket=minio.lake_bucket,
            key=blob_key,
            size_bytes=path.stat().st_size,
            content_type=content_type,
            created_at=now,
        )
        blob_id = blob_doc["id"]

    artifact = Artifact(
        kind="intermediate",
        blob_id=blob_id,
        batch_id=batch_id,
        run_id=run_id,
        created_at=now,
        producer=producer,
        label=label,
        parameters=parameters or {},
        content_type=content_type,
    )

    artifact_id = mongodb.insert_artifact(artifact.model_dump())

    return {
        "artifact_id": artifact_id,
        "blob_id": blob_id,
        "content_hash": content_hash,
        "blob_s3_path": f"s3://{blob_doc['bucket']}/{blob_doc['key']}",
    }
