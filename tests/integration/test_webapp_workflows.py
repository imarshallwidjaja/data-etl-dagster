import httpx
import os
import uuid
from urllib.parse import parse_qs, urlparse

import pytest
from minio import Minio

from .helpers import (
    cleanup_minio_manifest,
    cleanup_mongodb_manifest,
    cleanup_mongodb_runs_by_batch_id,
)

# Authentication
AUTH = ("admin", "admin")
BASE_URL = os.getenv("WEBAPP_URL", "http://localhost:8080")


@pytest.fixture
def client():
    with httpx.Client(base_url=BASE_URL, auth=AUTH, follow_redirects=True) as client:
        yield client


@pytest.fixture
def minio_client():
    return Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ROOT_USER", "minio"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minio_password"),
        secure=False,
    )


@pytest.mark.integration
def test_workflows_list_page(client):
    response = client.get("/workflows/")
    assert response.status_code == 200
    assert "Guided Workflows" in response.text
    assert "Ingest Vector Data" in response.text


@pytest.mark.integration
def test_workflow_start_wizard(client):
    response = client.get("/workflows/ingest-vector")
    assert response.status_code == 200
    assert "Ingest Vector Data" in response.text
    assert "Dataset Information" in response.text
    assert 'name="title"' in response.text


@pytest.mark.integration
def test_workflow_metadata_validation(client):
    # Submit step 0 without title
    response = client.post(
        "/workflows/ingest-vector/step/0", data={"_nav": "next", "description": "test"}
    )
    assert response.status_code == 200
    assert "Title is required" in response.text


@pytest.mark.integration
def test_workflow_step_navigation(client):
    # Submit step 0 with valid title
    response = client.post(
        "/workflows/ingest-vector/step/0",
        data={"_nav": "next", "title": "Test Dataset"},
    )
    assert response.status_code == 200
    assert "Select Files" in response.text
    assert 'name="_wizard_state"' in response.text
    assert "Test Dataset" in response.text


@pytest.mark.integration
def test_workflow_full_submission(
    client,
    minio_client,
    minio_settings,
    mongo_client,
    mongo_settings,
):
    import json

    batch_id = f"test_workflow_{uuid.uuid4().hex[:8]}"
    manifest_batch_id: str | None = None
    manifest_key: str | None = None

    # Accumulated state
    state = {
        "title": "Integration Test Dataset",
        "description": "Created by integration test",
        "file_path": "s3://landing-zone/test/data.geojson",
        "file_format": "GeoJSON",
        "file_type": "vector",
        "dataset_id": "ds_" + batch_id,
        "tags": {"testing": True},
    }

    try:
        # Submit last step with "submit" nav
        response = client.post(
            "/workflows/ingest-vector/step/2",
            data={
                "_nav": "submit",
                "_wizard_state": json.dumps(state),
                "dataset_id": state["dataset_id"],
                "project": "TEST",
            },
            follow_redirects=False,
        )

        assert response.status_code == 303
        assert "/workflows/ingest-vector/success" in response.headers["location"]
        assert "batch_id=" in response.headers["location"]

        location = response.headers["location"]
        parsed = urlparse(location)
        params = parse_qs(parsed.query)
        manifest_batch_id = params.get("batch_id", [None])[0]
        manifest_key = params.get("manifest_key", [None])[0]

        assert isinstance(manifest_batch_id, str) and manifest_batch_id
        assert isinstance(manifest_key, str) and manifest_key

        # Verify in MinIO
        objects = list(minio_client.list_objects("landing-zone", prefix=manifest_key))
        archived_objects = list(
            minio_client.list_objects("landing-zone", prefix=f"archive/{manifest_key}")
        )
        assert objects or archived_objects

    finally:
        if manifest_batch_id:
            cleanup_mongodb_manifest(mongo_client, mongo_settings, manifest_batch_id)
            cleanup_mongodb_runs_by_batch_id(
                mongo_client, mongo_settings, manifest_batch_id
            )
        if manifest_key:
            cleanup_minio_manifest(minio_client, minio_settings, manifest_key)
