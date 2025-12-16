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
        "format": "GTiff"
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
        "format": "CSV"
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
            "project": "ALPHA",
            "description": "Test tabular data",
            "tags": {"priority": 1, "source": "unit-test", "published": False},
            "join_config": {
                "left_key": "id",
                "right_key": "id",
                "how": "left",
            },
        }
    }


@pytest.fixture
def valid_tabular_manifest(valid_tabular_manifest_dict):
    """Complete valid tabular Manifest model instance."""
    return Manifest(**valid_tabular_manifest_dict)


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
            "project": "ALPHA",
            "description": "Test satellite imagery",
            "tags": {"priority": 1, "source": "unit-test", "published": False},
            "join_config": {
                "left_key": "parcel_id",
                "right_key": "parcel_id",
                "how": "left",
                "target_asset_id": "dataset_ab12cd34ef56",
            },
        }
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
        "status": "pending",
        "dagster_run_id": None,
        "error_message": None,
        "ingested_at": datetime(2024, 1, 1, 12, 0, 0),
        "completed_at": None
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
    return {
        "minx": -180.0,
        "miny": -90.0,
        "maxx": 180.0,
        "maxy": 90.0
    }


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
        "dagster_run_id": "run_12345",
        "kind": "spatial",
        "format": "geoparquet",
        "crs": "EPSG:4326",
        "bounds": valid_bounds_dict,
        "metadata": {
            "title": "Test Dataset",
            "description": "A test dataset for validation",
            "source": "Test Source",
            "license": "CC-BY-4.0",
            "tags": {}
        },
        "created_at": datetime(2024, 1, 1, 12, 0, 0),
        "updated_at": None
    }


@pytest.fixture
def valid_tabular_asset_dict():
    """Complete valid tabular asset dictionary."""
    return {
        "s3_key": "data-lake/dataset_tabular_001/v1/data.parquet",
        "dataset_id": "dataset_tabular_001",
        "version": 1,
        "content_hash": "sha256:" + "b" * 64,  # 64 hex chars
        "dagster_run_id": "run_12345",
        "kind": "tabular",
        "format": "parquet",
        "crs": None,
        "bounds": None,
        "metadata": {
            "title": "Test Tabular Dataset",
            "description": "A test tabular dataset",
            "source": "Test Source",
            "license": "CC-BY-4.0",
            "tags": {"project": "ALPHA"},
            "header_mapping": {"Original Name": "original_name", "Age (years)": "age_years"}
        },
        "created_at": datetime(2024, 1, 1, 12, 0, 0),
        "updated_at": None
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
        "source": "Test Source",
        "license": "CC-BY-4.0"
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

