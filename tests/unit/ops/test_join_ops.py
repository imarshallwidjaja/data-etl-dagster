# =============================================================================
# Unit Tests: Join Ops
# =============================================================================

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timezone

from libs.models import Asset, AssetKind, AssetMetadata, Bounds, OutputFormat, CRS, Manifest, JoinConfig

from services.dagster.etl_pipelines.ops.join_ops import (
    _resolve_join_assets,
    _load_tabular_to_postgis_from_file,
    _load_geoparquet_to_postgis,
    _execute_spatial_join,
)


# =============================================================================
# Test Data
# =============================================================================

SAMPLE_JOIN_MANIFEST = {
    "batch_id": "batch_join_001",
    "uploader": "test_user",
    "intent": "join_datasets",
    "files": [
        {
            "path": "s3://landing-zone/batch_join_001/data.csv",
            "type": "tabular",
            "format": "CSV"
        }
    ],
    "metadata": {
        "project": "TEST_PROJECT",
        "description": "Test join",
        "tags": {"dataset_id": "joined_dataset_001"},
        "join_config": {
            "target_asset_id": "507f1f77bcf86cd799439011",
            "left_key": "parcel_id",
            "right_key": "parcel_id",
            "how": "left",
        },
    }
}

SAMPLE_SPATIAL_ASSET = Asset(
    s3_key="data-lake/dataset_001/v1/data.parquet",
    dataset_id="dataset_001",
    version=1,
    content_hash="sha256:" + "a" * 64,
    dagster_run_id="run_123",
    kind=AssetKind.SPATIAL,
    format=OutputFormat.GEOPARQUET,
    crs=CRS("EPSG:4326"),
    bounds=Bounds(minx=0.0, miny=0.0, maxx=1.0, maxy=1.0),
    metadata=AssetMetadata(title="Test Spatial Dataset"),
    created_at=datetime.now(timezone.utc),
)


# =============================================================================
# Test: Resolve Join Assets
# =============================================================================

def test_resolve_join_assets_success():
    """Test successful resolution of join assets."""
    mock_mongodb = Mock()
    mock_log = Mock()
    
    mock_mongodb.get_asset_by_id.return_value = SAMPLE_SPATIAL_ASSET
    
    result = _resolve_join_assets(
        mongodb=mock_mongodb,
        manifest=SAMPLE_JOIN_MANIFEST,
        log=mock_log,
    )
    
    assert result["primary_type"] == "tabular"
    assert result["secondary_asset"]["dataset_id"] == "dataset_001"
    assert result["join_config"]["left_key"] == "parcel_id"
    mock_mongodb.get_asset_by_id.assert_called_once_with("507f1f77bcf86cd799439011")


def test_resolve_join_assets_missing_target():
    """Test fail-fast when target_asset_id is None."""
    mock_mongodb = Mock()
    mock_log = Mock()
    
    manifest_no_target = {
        **SAMPLE_JOIN_MANIFEST,
        "metadata": {
            **SAMPLE_JOIN_MANIFEST["metadata"],
            "join_config": {
                "left_key": "parcel_id",
                "right_key": "parcel_id",
                "how": "left",
            },
        },
    }
    
    with pytest.raises(ValueError, match="requires join_config.target_asset_id"):
        _resolve_join_assets(
            mongodb=mock_mongodb,
            manifest=manifest_no_target,
            log=mock_log,
        )


def test_resolve_join_assets_not_found():
    """Test error when asset ID doesn't exist."""
    mock_mongodb = Mock()
    mock_log = Mock()
    
    mock_mongodb.get_asset_by_id.return_value = None
    
    with pytest.raises(ValueError, match="Secondary asset not found"):
        _resolve_join_assets(
            mongodb=mock_mongodb,
            manifest=SAMPLE_JOIN_MANIFEST,
            log=mock_log,
        )


def test_resolve_join_assets_wrong_kind():
    """Test error when secondary is not spatial."""
    mock_mongodb = Mock()
    mock_log = Mock()
    
    tabular_asset = Asset(
        s3_key="data-lake/dataset_002/v1/data.parquet",
        dataset_id="dataset_002",
        version=1,
        content_hash="sha256:" + "b" * 64,
        dagster_run_id="run_456",
        kind=AssetKind.TABULAR,
        format=OutputFormat.PARQUET,
        crs=None,
        bounds=None,
        metadata=AssetMetadata(title="Test Tabular Dataset"),
        created_at=datetime.now(timezone.utc),
    )
    
    mock_mongodb.get_asset_by_id.return_value = tabular_asset
    
    with pytest.raises(ValueError, match="Secondary asset must be spatial"):
        _resolve_join_assets(
            mongodb=mock_mongodb,
            manifest=SAMPLE_JOIN_MANIFEST,
            log=mock_log,
        )


# =============================================================================
# Test: Execute Spatial Join
# =============================================================================

def test_execute_spatial_join_sql_left():
    """Verify SQL for LEFT JOIN."""
    mock_postgis = Mock()
    mock_log = Mock()
    
    tabular_info = {
        "schema": "proc_abc123",
        "table": "tabular_parent",
        "columns": ["parcel_id", "name", "value"],
    }
    
    spatial_info = {
        "schema": "proc_abc123",
        "table": "spatial_parent",
        "geom_column": "geom",
    }
    
    join_config = {
        "left_key": "parcel_id",
        "right_key": "parcel_id",
        "how": "left",
    }
    
    mock_postgis.get_table_bounds.return_value = Bounds(
        minx=0.0, miny=0.0, maxx=1.0, maxy=1.0
    )
    
    result = _execute_spatial_join(
        postgis=mock_postgis,
        tabular_info=tabular_info,
        spatial_info=spatial_info,
        join_config=join_config,
        output_table="joined",
        log=mock_log,
    )
    
    assert result["schema"] == "proc_abc123"
    assert result["table"] == "joined"
    assert result["bounds"] is not None
    
    # Verify SQL was called
    mock_postgis.execute_sql.assert_called_once()
    sql_call = mock_postgis.execute_sql.call_args[0][0]
    assert "LEFT JOIN" in sql_call
    assert 't."parcel_id" = s."parcel_id"' in sql_call


def test_execute_spatial_join_sql_inner():
    """Verify SQL for INNER JOIN."""
    mock_postgis = Mock()
    mock_log = Mock()
    
    tabular_info = {
        "schema": "proc_abc123",
        "table": "tabular_parent",
        "columns": ["id"],
    }
    
    spatial_info = {
        "schema": "proc_abc123",
        "table": "spatial_parent",
        "geom_column": "geom",
    }
    
    join_config = {
        "left_key": "id",
        "right_key": "id",
        "how": "inner",
    }
    
    mock_postgis.get_table_bounds.return_value = Bounds(
        minx=0.0, miny=0.0, maxx=1.0, maxy=1.0
    )
    
    result = _execute_spatial_join(
        postgis=mock_postgis,
        tabular_info=tabular_info,
        spatial_info=spatial_info,
        join_config=join_config,
        output_table="joined",
        log=mock_log,
    )
    
    sql_call = mock_postgis.execute_sql.call_args[0][0]
    assert "INNER JOIN" in sql_call

