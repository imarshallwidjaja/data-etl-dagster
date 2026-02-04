"""
Unit tests for raw archival activity logging.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from unittest.mock import Mock

from services.dagster.etl_pipelines.ops.raw_archival import archive_raw_source


def test_archive_raw_source_logs_activity_idempotent_upsert():
    content = b"raw-bytes"
    digest = hashlib.sha256(content).hexdigest()
    content_hash = f"sha256:{digest}"
    blob_key = f"blobs/sha256/{digest[:2]}/{digest}"

    minio = Mock()
    minio.lake_bucket = "data-lake"

    mongodb = Mock()
    mongodb.get_blob_by_hash.return_value = {
        "id": "blob123",
        "content_hash": content_hash,
        "bucket": "data-lake",
        "key": blob_key,
    }
    mongodb.insert_artifact.return_value = "artifact123"

    activity_collection = Mock()
    mongodb._get_collection.return_value = activity_collection

    for _ in range(2):
        archive_raw_source(
            minio=minio,
            mongodb=mongodb,
            source_s3_path=f"s3://data-lake/{blob_key}",
            batch_id="batch_001",
            uploader="uploader-1",
            run_id=None,
            log=Mock(),
        )

    assert activity_collection.update_one.call_count == 2

    expected_filter = {
        "action": "archive_raw_source",
        "resource_type": "artifact",
        "resource_id": "artifact123",
    }
    expected_details = {
        "artifact_id": "artifact123",
        "blob_id": "blob123",
        "batch_id": "batch_001",
        "content_hash": content_hash,
        "source_s3_path": f"s3://data-lake/{blob_key}",
        "blob_key": blob_key,
    }

    for call in activity_collection.update_one.call_args_list:
        filter_doc, update_doc = call.args

        assert filter_doc == expected_filter
        assert update_doc["$setOnInsert"]["action"] == "archive_raw_source"
        assert update_doc["$setOnInsert"]["resource_type"] == "artifact"
        assert update_doc["$setOnInsert"]["resource_id"] == "artifact123"
        assert update_doc["$setOnInsert"]["user"] == "uploader-1"
        assert update_doc["$setOnInsert"]["details"] == expected_details
        assert isinstance(update_doc["$setOnInsert"]["timestamp"], datetime)
        assert update_doc["$setOnInsert"]["timestamp"].tzinfo is not None
        assert call.kwargs["upsert"] is True
