import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.auth.dependencies import get_current_user
from app.main import app


client = TestClient(app)


@pytest.fixture
def mock_auth():
    mock_user = MagicMock(username="test_user", display_name="Test User")
    app.dependency_overrides[get_current_user] = lambda: mock_user
    yield mock_user
    app.dependency_overrides = {}


def _capture_upload_payload():
    captured: dict = {}

    def _capture(file, key, content_type):
        captured["key"] = key
        captured["content_type"] = content_type
        captured["body"] = json.loads(file.getvalue().decode("utf-8"))

    return captured, _capture


def test_rerun_rewrites_manifest_paths_to_latest_blob(mock_auth):
    archived_manifest = {
        "batch_id": "batch_1",
        "files": [
            {"path": "s3://landing/raw/file1.csv", "type": "tabular", "format": "CSV"},
            {"path": "s3://landing/raw/file2.csv", "type": "tabular", "format": "CSV"},
        ],
    }

    artifacts = [
        {
            "id": "a1",
            "blob_id": "b1",
            "batch_id": "batch_1",
            "created_at": datetime(2024, 1, 1),
            "source_s3_path": "s3://landing/raw/file1.csv",
        },
        {
            "id": "a2",
            "blob_id": "b2",
            "batch_id": "batch_1",
            "created_at": datetime(2024, 1, 2),
            "source_s3_path": "s3://landing/raw/file1.csv",
        },
        {
            "id": "a3",
            "blob_id": "b3",
            "batch_id": "batch_1",
            "created_at": datetime(2024, 1, 1),
            "source_s3_path": "s3://landing/raw/file2.csv",
        },
    ]

    blobs_by_id = {
        "b2": {"_id": "b2", "bucket": "data-lake", "key": "blobs/file1.parquet"},
        "b3": {"_id": "b3", "bucket": "data-lake", "key": "blobs/file2.parquet"},
    }

    captured, capture_upload = _capture_upload_payload()
    mock_minio = MagicMock()
    mock_minio.get_archived_manifest.return_value = archived_manifest
    mock_minio.upload_if_not_exists.side_effect = capture_upload

    mock_mongodb = MagicMock()
    mock_mongodb.list_raw_source_artifacts_for_batch.return_value = artifacts
    mock_mongodb.get_blobs_by_ids.return_value = blobs_by_id

    with (
        patch("app.routers.manifests.get_minio_service", return_value=mock_minio),
        patch("app.routers.manifests.get_mongodb_service", return_value=mock_mongodb),
        patch("app.routers.manifests.create_rerun_batch_id", return_value="batch_1_v2"),
        patch("app.routers.manifests.get_activity_service"),
    ):
        response = client.post("/manifests/batch_1/rerun")

    assert response.status_code == 200
    uploaded_manifest = captured["body"]
    assert uploaded_manifest["batch_id"] == "batch_1_v2"
    assert uploaded_manifest["files"][0]["path"] == "s3://data-lake/blobs/file1.parquet"
    assert uploaded_manifest["files"][1]["path"] == "s3://data-lake/blobs/file2.parquet"


def test_rerun_leaves_paths_when_blob_missing(mock_auth):
    archived_manifest = {
        "batch_id": "batch_missing",
        "files": [
            {"path": "s3://landing/raw/missing.csv", "type": "tabular", "format": "CSV"}
        ],
    }

    artifacts = [
        {
            "id": "a1",
            "blob_id": "b-missing",
            "batch_id": "batch_missing",
            "created_at": datetime(2024, 2, 1),
            "source_s3_path": "s3://landing/raw/missing.csv",
        }
    ]

    captured, capture_upload = _capture_upload_payload()
    mock_minio = MagicMock()
    mock_minio.get_archived_manifest.return_value = archived_manifest
    mock_minio.upload_if_not_exists.side_effect = capture_upload

    mock_mongodb = MagicMock()
    mock_mongodb.list_raw_source_artifacts_for_batch.return_value = artifacts
    mock_mongodb.get_blobs_by_ids.return_value = {}

    with (
        patch("app.routers.manifests.get_minio_service", return_value=mock_minio),
        patch("app.routers.manifests.get_mongodb_service", return_value=mock_mongodb),
        patch(
            "app.routers.manifests.create_rerun_batch_id",
            return_value="batch_missing_v2",
        ),
        patch("app.routers.manifests.get_activity_service"),
    ):
        response = client.post("/manifests/batch_missing/rerun")

    assert response.status_code == 200
    uploaded_manifest = captured["body"]
    assert uploaded_manifest["files"][0]["path"] == "s3://landing/raw/missing.csv"
