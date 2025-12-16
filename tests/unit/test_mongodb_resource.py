"""
Unit tests for MongoDBResource.

Uses mongomock to exercise MongoDB operations without a live service.
"""

from datetime import datetime, timezone

import mongomock
import pytest

from libs.models import (
    Asset,
    AssetMetadata,
    Bounds,
    FileEntry,
    FileType,
    Manifest,
    ManifestMetadata,
    ManifestRecord,
    ManifestStatus,
    OutputFormat,
)

from services.dagster.etl_pipelines.resources import MongoDBResource


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mongomock_client():
    """In-memory MongoDB client for tests."""
    return mongomock.MongoClient()


@pytest.fixture
def mongo_resource(monkeypatch, mongomock_client):
    """MongoDBResource configured to use the mongomock client."""
    monkeypatch.setattr(
        "services.dagster.etl_pipelines.resources.mongodb_resource.MongoClient",
        lambda *args, **kwargs: mongomock_client,
    )
    return MongoDBResource(connection_string="mongodb://localhost:27017")


@pytest.fixture
def manifest_record():
    files = [
        FileEntry(
            path="s3://landing-zone/batch_001/dataset.gpkg",
            type=FileType.VECTOR,
            format="GPKG",
        )
    ]
    manifest = Manifest(
        batch_id="batch_001",
        uploader="test_user",
        intent="ingest_vector",
        files=files,
        metadata=ManifestMetadata(project="ALPHA", description="Test batch"),
    )
    return ManifestRecord.from_manifest(manifest)


@pytest.fixture
def asset():
    return Asset(
        s3_key="data-lake/batch_001/v1/data.parquet",
        dataset_id="batch_001",
        version=1,
        content_hash=f"sha256:{'a' * 64}",
        dagster_run_id="run_123",
        kind="spatial",
        format=OutputFormat.GEOPARQUET,
        crs="EPSG:4326",
        bounds=Bounds(minx=0.0, miny=0.0, maxx=1.0, maxy=1.0),
        metadata=AssetMetadata(title="Test Dataset"),
        created_at=datetime.now(timezone.utc),
    )


# =============================================================================
# Tests
# =============================================================================


def test_insert_and_get_manifest(mongo_resource, manifest_record):
    inserted_id = mongo_resource.insert_manifest(manifest_record)
    assert inserted_id is not None

    retrieved = mongo_resource.get_manifest(manifest_record.batch_id)
    assert retrieved is not None
    assert retrieved.batch_id == manifest_record.batch_id
    assert retrieved.status == manifest_record.status


def test_update_manifest_status(mongo_resource, manifest_record, mongomock_client):
    mongo_resource.insert_manifest(manifest_record)

    mongo_resource.update_manifest_status(
        manifest_record.batch_id,
        ManifestStatus.COMPLETED,
        dagster_run_id="run_123",
        error_message="All good",
    )

    stored = mongomock_client[mongo_resource.database][MongoDBResource.MANIFESTS].find_one(
        {"batch_id": manifest_record.batch_id}
    )
    assert stored["status"] == ManifestStatus.COMPLETED.value
    assert stored["dagster_run_id"] == "run_123"
    assert stored.get("completed_at") is not None


def test_insert_asset_and_asset_exists(mongo_resource, asset):
    inserted_id = mongo_resource.insert_asset(asset)
    assert inserted_id is not None

    assert mongo_resource.asset_exists(asset.content_hash)
    assert not mongo_resource.asset_exists(f"sha256:{'b' * 64}")


def test_get_asset_and_next_version(mongo_resource, asset):
    mongo_resource.insert_asset(asset)
    latest = mongo_resource.get_asset(asset.dataset_id, asset.version)
    assert latest is not None
    assert latest.dataset_id == asset.dataset_id

    next_version = mongo_resource.get_next_version(asset.dataset_id)
    assert next_version == asset.version + 1


def test_get_latest_asset_returns_highest_version(mongo_resource, asset):
    mongo_resource.insert_asset(asset)
    updated_asset = asset.model_copy(update={"version": 2})
    mongo_resource.insert_asset(updated_asset)

    latest = mongo_resource.get_latest_asset(asset.dataset_id)
    assert latest is not None
    assert latest.version == 2


def test_insert_asset_excludes_none_bounds(mongo_resource):
    """Test that insert_asset excludes None bounds from MongoDB document."""
    asset_with_none_bounds = Asset(
        s3_key="data-lake/test/v1/data.parquet",
        dataset_id="test_dataset_none_bounds",
        version=1,
        content_hash="sha256:abc123def4567890abcdef1234567890abcdef1234567890abcdef1234567890",
        dagster_run_id="test_run_id",
        format=OutputFormat.GEOPARQUET,
        crs="EPSG:4326",
        bounds=None,  # None bounds should be excluded
        metadata=AssetMetadata(
            title="Test Asset",
            description="Test asset with None bounds",
        ),
        created_at=datetime.now(timezone.utc),
        updated_at=None,
    )

    inserted_id = mongo_resource.insert_asset(asset_with_none_bounds)
    assert inserted_id is not None

    # Retrieve the document and verify bounds field is not present
    retrieved = mongo_resource.get_asset("test_dataset_none_bounds", 1)
    assert retrieved is not None
    assert retrieved.bounds is None  # Model should have None bounds

    # Verify the underlying document doesn't contain the bounds field
    collection = mongo_resource._get_collection(mongo_resource.ASSETS)
    document = collection.find_one({"dataset_id": "test_dataset_none_bounds", "version": 1})
    assert document is not None
    assert "bounds" not in document  # bounds field should be excluded when None
