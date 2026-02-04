"""
Unit tests for intermediate artifact registration helpers.

Focus: parameter normalization + hash-first dedup (no duplicate blob upload).
"""

from __future__ import annotations

import hashlib
from unittest.mock import Mock

from services.dagster.etl_pipelines.ops.intermediate_artifacts import (
    register_intermediate_from_local_file,
)


def test_register_intermediate_normalizes_parameters(tmp_path):
    content = b"intermediate-bytes"
    local_path = tmp_path / "output.parquet"
    local_path.write_bytes(content)

    minio = Mock()
    minio.lake_bucket = "data-lake"

    mongodb = Mock()
    mongodb.get_blob_by_hash.return_value = {
        "_id": "blob456",
        "id": "blob456",
        "content_hash": f"sha256:{hashlib.sha256(content).hexdigest()}",
        "bucket": "data-lake",
        "key": "blobs/sha256/aa/placeholder",
    }

    register_intermediate_from_local_file(
        local_path=str(local_path),
        batch_id="batch_001",
        run_id="run_001",
        producer="transform_op",
        label="normalized",
        parameters=None,
        content_type="application/octet-stream",
        minio=minio,
        mongodb=mongodb,
        log=Mock(),
    )

    inserted_artifact = mongodb.insert_artifact.call_args[0][0]
    assert inserted_artifact["parameters"] == {}


def test_register_intermediate_skips_upload_when_blob_exists(tmp_path):
    content = b"intermediate-bytes"
    local_path = tmp_path / "output.parquet"
    local_path.write_bytes(content)

    digest = hashlib.sha256(content).hexdigest()
    content_hash = f"sha256:{digest}"

    minio = Mock()
    minio.lake_bucket = "data-lake"

    mongodb = Mock()
    mongodb.get_blob_by_hash.return_value = {
        "_id": "blob123",
        "id": "blob123",
        "content_hash": content_hash,
        "bucket": "data-lake",
        "key": f"blobs/sha256/{digest[:2]}/{digest}",
    }

    result = register_intermediate_from_local_file(
        local_path=str(local_path),
        batch_id="batch_001",
        run_id=None,
        producer="transform_op",
        label="normalized",
        parameters={"stage": "clean"},
        content_type=None,
        minio=minio,
        mongodb=mongodb,
        log=Mock(),
    )

    mongodb.get_blob_by_hash.assert_called_once_with(content_hash)
    minio.upload_to_lake.assert_not_called()
    mongodb.insert_artifact.assert_called_once()
    assert result["blob_id"] == "blob123"
    assert result["content_hash"] == content_hash
