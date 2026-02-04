"""
Unit tests for raw archival helpers.

Focus: hash-first dedup (no duplicate blob upload).
"""

from __future__ import annotations

import hashlib
from unittest.mock import Mock

from services.dagster.etl_pipelines.ops.raw_archival import archive_raw_source


class _FakeResponse:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    def stream(self, _size: int):
        for chunk in self._chunks:
            yield chunk

    def close(self) -> None:
        return None

    def release_conn(self) -> None:
        return None


def test_archive_raw_source_skips_upload_when_blob_exists():
    content = b"raw-bytes"
    digest = hashlib.sha256(content).hexdigest()
    content_hash = f"sha256:{digest}"

    minio = Mock()
    minio.lake_bucket = "data-lake"
    minio.get_client.return_value.get_object.return_value = _FakeResponse([content])

    mongodb = Mock()
    mongodb.get_blob_by_hash.return_value = {
        "_id": "blob123",
        "id": "blob123",
        "content_hash": content_hash,
        "bucket": "data-lake",
        "key": f"blobs/sha256/{digest[:2]}/{digest}",
    }

    result = archive_raw_source(
        minio=minio,
        mongodb=mongodb,
        source_s3_path="s3://landing-zone/batch_001/data.csv",
        batch_id="batch_001",
        uploader="test_user",
        run_id=None,
        log=Mock(),
    )

    mongodb.get_blob_by_hash.assert_called_once_with(content_hash)
    minio.upload_to_lake.assert_not_called()
    mongodb.insert_artifact.assert_called_once()
    assert result["blob_id"] == "blob123"
    assert result["content_hash"] == content_hash
