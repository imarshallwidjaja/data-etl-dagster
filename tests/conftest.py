"""
Shared pytest fixtures for model tests.

Provides reusable test data fixtures to avoid duplication across test files.
"""

import pytest
from datetime import datetime

from libs.models import (
    FileEntry,
    ManifestMetadata,
    Manifest,
    ManifestStatus,
    ManifestRecord,
    AssetKind,
    AssetMetadata,
    Asset,
    Bounds,
    FileType,
    OutputFormat,
)


# =============================================================================
# File Entry Fixtures
# =============================================================================


@pytest.fixture
def valid_file_entry_dict():
    """Single valid file entry dictionary."""
    return {
        "path": "s3://landing-zone/batch_001/image.tif",
        "type": "raster",
        "format": "GTiff",
    }


@pytest.fixture
def valid_file_entry(valid_file_entry_dict):
    """Single valid FileEntry model instance."""
    return FileEntry(**valid_file_entry_dict)


@pytest.fixture
def valid_tabular_file_entry_dict():
    """Single valid tabular file entry dictionary."""
    return {
        "path": "s3://landing-zone/batch_001/data.csv",
        "type": "tabular",
        "format": "CSV",
    }


@pytest.fixture
def valid_tabular_file_entry(valid_tabular_file_entry_dict):
    """Single valid tabular FileEntry model instance."""
    return FileEntry(**valid_tabular_file_entry_dict)


@pytest.fixture
def valid_tabular_manifest_dict(valid_tabular_file_entry_dict):
    """Complete valid tabular manifest dictionary."""
    return {
        "batch_id": "batch_tabular_001",
        "uploader": "user_123",
        "intent": "ingest_tabular",
        "files": [valid_tabular_file_entry_dict],
        "metadata": {
            "title": "Test Tabular Dataset",
            "description": "Test tabular data for validation",
            "keywords": ["tabular", "test"],
            "source": "Unit Test Suite",
            "license": "MIT",
            "attribution": "Test Contributors",
            "project": "ALPHA",
            "tags": {"priority": 1, "source": "unit-test", "published": False},
        },
    }


@pytest.fixture
def valid_tabular_manifest(valid_tabular_manifest_dict):
    """Complete valid tabular Manifest model instance."""
    return Manifest(**valid_tabular_manifest_dict)


# =============================================================================
# Join Manifest Fixtures
# =============================================================================


@pytest.fixture
def valid_join_manifest_dict(valid_tabular_file_entry_dict):
    """Complete valid join manifest dictionary."""
    return {
        "batch_id": "batch_join_001",
        "uploader": "test_user",
        "intent": "join_datasets",
        "files": [valid_tabular_file_entry_dict],
        "metadata": {
            "project": "JOIN_TEST",
            "description": "Test join manifest",
            "tags": {"dataset_id": "joined_dataset_001"},
            "join_config": {
                "target_asset_id": "507f1f77bcf86cd799439011",
                "left_key": "parcel_id",
                "right_key": "parcel_id",
                "how": "left",
            },
        },
    }


@pytest.fixture
def valid_join_manifest(valid_join_manifest_dict):
    """Complete valid join Manifest model instance."""
    return Manifest(**valid_join_manifest_dict)


@pytest.fixture
def valid_spatial_asset(valid_bounds_dict):
    """Spatial asset suitable for join testing."""
    return Asset(
        s3_key="data-lake/spatial_001/v1/data.parquet",
        dataset_id="spatial_001",
        version=1,
        content_hash="sha256:" + "a" * 64,
        run_id="507f1f77bcf86cd799439011",
        kind=AssetKind.SPATIAL,
        format=OutputFormat.GEOPARQUET,
        crs="EPSG:4326",
        bounds=Bounds(**valid_bounds_dict),
        metadata=AssetMetadata(
            title="Test Spatial Dataset",
            description="Spatial dataset for testing",
            keywords=["spatial", "test"],
            source="Test Source",
            license="MIT",
            attribution="Test Team",
            geometry_type="MULTIPOLYGON",
            column_schema={
                "geom": {
                    "title": "geom",
                    "type_name": "GEOMETRY",
                    "logical_type": "geometry",
                    "nullable": False,
                }
            },
        ),
        created_at=datetime(2024, 1, 1, 12, 0, 0),
    )


# =============================================================================
# Manifest Fixtures
# =============================================================================


@pytest.fixture
def valid_manifest_dict(valid_file_entry_dict):
    """Complete valid manifest dictionary."""
    return {
        "batch_id": "batch_001",
        "uploader": "user_123",
        "intent": "ingest_satellite_raster",
        "files": [valid_file_entry_dict],
        "metadata": {
            "title": "Test Satellite Dataset",
            "description": "Test satellite imagery for validation",
            "keywords": ["satellite", "test"],
            "source": "Unit Test Suite",
            "license": "MIT",
            "attribution": "Test Contributors",
            "project": "ALPHA",
            "tags": {"priority": 1, "source": "unit-test", "published": False},
        },
    }


@pytest.fixture
def valid_manifest(valid_manifest_dict):
    """Complete valid Manifest model instance."""
    return Manifest(**valid_manifest_dict)


@pytest.fixture
def valid_manifest_record_dict(valid_manifest_dict):
    """Persisted manifest with status/timestamps."""
    return {
        **valid_manifest_dict,
        "status": "running",
        "error_message": None,
        "ingested_at": datetime(2024, 1, 1, 12, 0, 0),
        "completed_at": None,
    }


@pytest.fixture
def valid_manifest_record(valid_manifest_record_dict):
    """Complete valid ManifestRecord model instance."""
    return ManifestRecord(**valid_manifest_record_dict)


# =============================================================================
# Asset Fixtures
# =============================================================================


@pytest.fixture
def valid_bounds_dict():
    """Valid bounds object dictionary."""
    return {"minx": -180.0, "miny": -90.0, "maxx": 180.0, "maxy": 90.0}


@pytest.fixture
def valid_bounds(valid_bounds_dict):
    """Valid Bounds model instance."""
    return Bounds(**valid_bounds_dict)


@pytest.fixture
def valid_asset_dict(valid_bounds_dict):
    """Complete valid asset dictionary."""
    return {
        "s3_key": "data-lake/dataset_001/v1/data.parquet",
        "dataset_id": "dataset_001",
        "version": 1,
        "content_hash": "sha256:" + "a" * 64,  # 64 hex chars
        "run_id": "507f1f77bcf86cd799439011",
        "kind": "spatial",
        "format": "geoparquet",
        "crs": "EPSG:4326",
        "bounds": valid_bounds_dict,
        "metadata": {
            "title": "Test Dataset",
            "description": "A test dataset for validation",
            "keywords": ["test", "spatial"],
            "source": "Test Source",
            "license": "CC-BY-4.0",
            "attribution": "Test Team",
            "tags": {},
            "geometry_type": "MULTIPOLYGON",
            "column_schema": {
                "geom": {
                    "title": "geom",
                    "type_name": "GEOMETRY",
                    "logical_type": "geometry",
                    "nullable": False,
                }
            },
        },
        "created_at": datetime(2024, 1, 1, 12, 0, 0),
        "updated_at": None,
    }


@pytest.fixture
def valid_tabular_asset_dict():
    """Complete valid tabular asset dictionary."""
    return {
        "s3_key": "data-lake/dataset_tabular_001/v1/data.parquet",
        "dataset_id": "dataset_tabular_001",
        "version": 1,
        "content_hash": "sha256:" + "b" * 64,  # 64 hex chars
        "run_id": "507f1f77bcf86cd799439012",
        "kind": "tabular",
        "format": "parquet",
        "crs": None,
        "bounds": None,
        "metadata": {
            "title": "Test Tabular Dataset",
            "description": "A test tabular dataset",
            "keywords": ["tabular", "test"],
            "source": "Test Source",
            "license": "CC-BY-4.0",
            "attribution": "Test Team",
            "tags": {"project": "ALPHA"},
            "header_mapping": {
                "Original Name": "original_name",
                "Age (years)": "age_years",
            },
            "column_schema": {
                "original_name": {
                    "title": "Original Name",
                    "type_name": "STRING",
                    "logical_type": "string",
                },
                "age_years": {
                    "title": "Age (years)",
                    "type_name": "INTEGER",
                    "logical_type": "int64",
                },
            },
        },
        "created_at": datetime(2024, 1, 1, 12, 0, 0),
        "updated_at": None,
    }


@pytest.fixture
def valid_tabular_asset(valid_tabular_asset_dict):
    """Complete valid tabular Asset model instance."""
    return Asset(**valid_tabular_asset_dict)


@pytest.fixture
def valid_asset(valid_asset_dict):
    """Complete valid Asset model instance."""
    return Asset(**valid_asset_dict)


@pytest.fixture
def valid_asset_metadata_dict():
    """Valid asset metadata dictionary."""
    return {
        "title": "Test Dataset",
        "description": "A test dataset",
        "keywords": ["test"],
        "source": "Test Source",
        "license": "CC-BY-4.0",
        "attribution": "Test Team",
    }


@pytest.fixture
def valid_asset_metadata(valid_asset_metadata_dict):
    """Valid AssetMetadata model instance."""
    return AssetMetadata(**valid_asset_metadata_dict)


# =============================================================================
# CRS Test Data Fixtures
# =============================================================================


@pytest.fixture
def sample_wkt_crs():
    """Example WKT string for CRS tests."""
    return (
        'PROJCS["WGS 84 / Pseudo-Mercator",'
        'GEOGCS["WGS 84",'
        'DATUM["WGS_1984",'
        'SPHEROID["WGS 84",6378137,298.257223563]],'
        'PRIMEM["Greenwich",0],'
        'UNIT["degree",0.0174532925199433]],'
        'PROJECTION["Mercator_1SP"],'
        'PARAMETER["central_meridian",0],'
        'PARAMETER["scale_factor",1],'
        'PARAMETER["false_easting",0],'
        'PARAMETER["false_northing",0],'
        'UNIT["metre",1],'
        'AXIS["X",EAST],'
        'AXIS["Y",NORTH]]'
    )


@pytest.fixture
def sample_proj_crs():
    """Example PROJ string for CRS tests."""
    return "+proj=utm +zone=55 +south +ellps=GRS80 +datum=GDA94 +units=m +no_defs"


# =============================================================================
# MongoDB Test Isolation Fixtures
# =============================================================================


@pytest.fixture
def mongo_database():
    """Get MongoDB database for tests that need direct database access.

    Includes connectivity verification to fail fast with clear error message.
    """
    from pymongo import MongoClient
    from pymongo.errors import ServerSelectionTimeoutError

    from libs.models import MongoSettings

    settings = MongoSettings()
    client = MongoClient(settings.connection_string, serverSelectionTimeoutMS=5000)

    # Force connection verification - fail fast with clear message
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError as e:
        pytest.skip(f"MongoDB not available: {e}")

    yield client[settings.database]
    client.close()


@pytest.fixture
def clean_mongodb(mongo_database):
    """Provide clean MongoDB state by clearing all non-system collections.

    This fixture preserves schema validators and indexes (created by migrations)
    while clearing all business data for test isolation.

    Uses pre-cleanup only pattern: relies on next test's pre-cleanup to handle
    any data left behind. This exposes test pollution issues rather than hiding them.
    """
    # Exclude system/special collections from cleanup
    EXCLUDED_COLLECTIONS = {"system.indexes", "schema_migrations"}

    # Get all collections and clean them BEFORE the test
    all_collections = set(mongo_database.list_collection_names())
    collections_to_clean = all_collections - EXCLUDED_COLLECTIONS

    for coll in collections_to_clean:
        mongo_database[coll].delete_many({})

    yield mongo_database
    # NO post-cleanup - rely on next test's pre-cleanup


# =============================================================================
# MinIO Test Isolation Fixtures
# =============================================================================


@pytest.fixture
def clean_minio(minio_client):
    """Ensure MinIO buckets used in tests are empty before test.

    WARNING: Destructive! Only clears test buckets.

    Uses pre-cleanup only pattern with specific error handling:
    - NoSuchBucket/NoSuchKey are expected for fresh environments
    - Other S3 errors are re-raised to surface real issues
    """
    from minio.error import S3Error

    buckets = ["landing-zone", "data-lake", "archive"]

    def _clean():
        for bucket in buckets:
            try:
                objects = list(minio_client.list_objects(bucket, recursive=True))
                for obj in objects:
                    minio_client.remove_object(bucket, obj.object_name)
            except S3Error as e:
                if e.code in ("NoSuchBucket", "NoSuchKey"):
                    continue  # Expected for fresh/empty buckets
                raise  # Unexpected errors should fail the test

    _clean()
    yield minio_client
    # NO post-cleanup - rely on next test's pre-cleanup
