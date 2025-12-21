"""
Unit tests for MongoDBResource.

Uses mongomock to exercise MongoDB operations without a live service.
"""

from datetime import datetime, timezone

from bson import ObjectId
import mongomock
import pytest

from libs.models import (
    Asset,
    AssetKind,
    AssetMetadata,
    Bounds,
    FileEntry,
    FileType,
    Manifest,
    ManifestMetadata,
    ManifestRecord,
    ManifestStatus,
    OutputFormat,
    Run,
    RunStatus,
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
        run_id="507f1f77bcf86cd799439011",
        kind=AssetKind.SPATIAL,
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
        ManifestStatus.SUCCESS,
        error_message="All good",
    )

    stored = mongomock_client[mongo_resource.database][
        MongoDBResource.MANIFESTS
    ].find_one({"batch_id": manifest_record.batch_id})
    assert stored["status"] == ManifestStatus.SUCCESS.value
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
        run_id="507f1f77bcf86cd799439011",
        kind=AssetKind.SPATIAL,
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
    document = collection.find_one(
        {"dataset_id": "test_dataset_none_bounds", "version": 1}
    )
    assert document is not None
    assert "bounds" not in document  # bounds field should be excluded when None


def test_get_asset_by_id_returns_asset(mongo_resource, asset):
    inserted_id = mongo_resource.insert_asset(asset)
    retrieved = mongo_resource.get_asset_by_id(inserted_id)
    assert retrieved is not None
    assert retrieved.dataset_id == asset.dataset_id
    assert retrieved.version == asset.version


def test_get_asset_by_id_returns_none_for_invalid_id(mongo_resource):
    assert mongo_resource.get_asset_by_id("not_an_object_id") is None


def test_insert_lineage_stores_object_ids(mongo_resource, mongomock_client, asset):
    # First create a run to get a run_id
    run_id = mongo_resource.insert_run(
        dagster_run_id="dagster_run_lineage_test",
        batch_id="batch_lineage",
        job_name="test_job",
    )

    source_id = mongo_resource.insert_asset(asset)
    target_id = mongo_resource.insert_asset(
        asset.model_copy(update={"dataset_id": "child_dataset"})
    )

    lineage_id = mongo_resource.insert_lineage(
        source_asset_id=source_id,
        target_asset_id=target_id,
        run_id=run_id,
        transformation="spatial_join",
        parameters={"how": "left"},
    )
    assert lineage_id is not None

    stored = mongomock_client[mongo_resource.database][
        MongoDBResource.LINEAGE
    ].find_one({"_id": ObjectId(lineage_id)})
    # mongomock uses bson ObjectId; verify key fields exist
    assert stored is not None
    assert str(stored["source_asset_id"]) == source_id
    assert str(stored["target_asset_id"]) == target_id
    assert str(stored["run_id"]) == run_id
    assert stored["transformation"] == "spatial_join"


# =============================================================================
# Run Operation Tests
# =============================================================================


def test_insert_run_creates_document(mongo_resource, mongomock_client):
    """Test that insert_run creates a run document with correct fields."""
    run_id = mongo_resource.insert_run(
        dagster_run_id="dagster_run_abc123",
        batch_id="batch_001",
        job_name="spatial_asset_job",
        partition_key="test_dataset",
    )
    assert run_id is not None

    stored = mongomock_client[mongo_resource.database][MongoDBResource.RUNS].find_one(
        {"dagster_run_id": "dagster_run_abc123"}
    )
    assert stored is not None
    assert stored["batch_id"] == "batch_001"
    assert stored["job_name"] == "spatial_asset_job"
    assert stored["partition_key"] == "test_dataset"
    assert stored["status"] == RunStatus.RUNNING.value
    assert stored["asset_ids"] == []
    assert stored["started_at"] is not None
    assert stored["completed_at"] is None


def test_insert_run_upsert_is_idempotent(mongo_resource, mongomock_client):
    """Test that insert_run is idempotent (calling twice returns same ObjectId)."""
    run_id_1 = mongo_resource.insert_run(
        dagster_run_id="dagster_run_xyz789",
        batch_id="batch_002",
        job_name="tabular_asset_job",
    )
    run_id_2 = mongo_resource.insert_run(
        dagster_run_id="dagster_run_xyz789",
        batch_id="batch_002",
        job_name="tabular_asset_job",
    )
    assert run_id_1 == run_id_2

    # Should only have one document
    count = mongomock_client[mongo_resource.database][
        MongoDBResource.RUNS
    ].count_documents({"dagster_run_id": "dagster_run_xyz789"})
    assert count == 1


def test_update_run_status_success(mongo_resource, mongomock_client):
    """Test that update_run_status updates status and sets completed_at."""
    mongo_resource.insert_run(
        dagster_run_id="dagster_run_status_test",
        batch_id="batch_003",
        job_name="join_asset_job",
    )

    mongo_resource.update_run_status(
        dagster_run_id="dagster_run_status_test",
        status=RunStatus.SUCCESS,
    )

    stored = mongomock_client[mongo_resource.database][MongoDBResource.RUNS].find_one(
        {"dagster_run_id": "dagster_run_status_test"}
    )
    assert stored["status"] == RunStatus.SUCCESS.value
    assert stored["completed_at"] is not None


def test_update_run_status_failure_with_error(mongo_resource, mongomock_client):
    """Test that update_run_status stores error message on failure."""
    mongo_resource.insert_run(
        dagster_run_id="dagster_run_failure_test",
        batch_id="batch_004",
        job_name="spatial_asset_job",
    )

    mongo_resource.update_run_status(
        dagster_run_id="dagster_run_failure_test",
        status=RunStatus.FAILURE,
        error_message="Something went wrong",
    )

    stored = mongomock_client[mongo_resource.database][MongoDBResource.RUNS].find_one(
        {"dagster_run_id": "dagster_run_failure_test"}
    )
    assert stored["status"] == RunStatus.FAILURE.value
    assert stored["error_message"] == "Something went wrong"
    assert stored["completed_at"] is not None


def test_get_run_returns_run_model(mongo_resource):
    """Test that get_run returns a Run model instance."""
    mongo_resource.insert_run(
        dagster_run_id="dagster_run_gettest",
        batch_id="batch_005",
        job_name="tabular_asset_job",
        partition_key="my_dataset",
    )

    run = mongo_resource.get_run("dagster_run_gettest")
    assert run is not None
    assert isinstance(run, Run)
    assert run.dagster_run_id == "dagster_run_gettest"
    assert run.batch_id == "batch_005"
    assert run.job_name == "tabular_asset_job"
    assert run.partition_key == "my_dataset"
    assert run.status == RunStatus.RUNNING


def test_get_run_object_id(mongo_resource):
    """Test that get_run_object_id returns ObjectId string."""
    inserted_id = mongo_resource.insert_run(
        dagster_run_id="dagster_run_oid_test",
        batch_id="batch_006",
        job_name="join_asset_job",
    )

    retrieved_id = mongo_resource.get_run_object_id("dagster_run_oid_test")
    assert retrieved_id == inserted_id


def test_add_asset_to_run(mongo_resource, asset, mongomock_client):
    """Test that add_asset_to_run appends asset ObjectId to run document."""
    mongo_resource.insert_run(
        dagster_run_id="dagster_run_asset_link",
        batch_id="batch_007",
        job_name="spatial_asset_job",
    )

    asset_id = mongo_resource.insert_asset(asset)
    mongo_resource.add_asset_to_run("dagster_run_asset_link", asset_id)

    stored = mongomock_client[mongo_resource.database][MongoDBResource.RUNS].find_one(
        {"dagster_run_id": "dagster_run_asset_link"}
    )
    assert len(stored["asset_ids"]) == 1
    assert str(stored["asset_ids"][0]) == asset_id
