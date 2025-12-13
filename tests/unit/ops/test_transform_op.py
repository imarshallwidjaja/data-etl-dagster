# =============================================================================
# Unit Tests: Transform Op
# =============================================================================

import pytest
from unittest.mock import Mock, patch, PropertyMock
from dagster import build_op_context

from services.dagster.etl_pipelines.ops.transform_op import spatial_transform, _spatial_transform
from libs.models import Bounds


# =============================================================================
# Test Data
# =============================================================================

SAMPLE_SCHEMA_INFO = {
    "schema": "proc_abc12345_def6_7890_abcd_ef1234567890",
    "manifest": {
        "batch_id": "batch_001",
        "uploader": "test_user",
        "intent": "ingest_vector",
        "files": [
            {
                "path": "s3://landing-zone/batch_001/data.geojson",
                "type": "vector",
                "format": "GeoJSON",
                "crs": "EPSG:3857"
            }
        ],
        "metadata": {
            "project": "TEST_PROJECT",
            "description": "Test dataset"
        }
    },
    "tables": ["raw_data"],
    "run_id": "abc12345-def6-7890-abcd-ef1234567890",
}


# =============================================================================
# Test: Core Logic (_spatial_transform)
# =============================================================================

def test_spatial_transform_success():
    """Test successful spatial transformation."""
    mock_postgis = Mock()
    mock_postgis.table_exists.return_value = True
    mock_postgis.execute_sql = Mock()
    mock_postgis.get_table_bounds.return_value = Bounds(
        minx=-180.0,
        miny=-90.0,
        maxx=180.0,
        maxy=90.0,
    )
    
    mock_log = Mock()
    
    # Call core function
    result = _spatial_transform(
        postgis=mock_postgis,
        schema_info=SAMPLE_SCHEMA_INFO,
        log=mock_log,
    )
    
    # Verify result
    assert result["schema"] == "proc_abc12345_def6_7890_abcd_ef1234567890"
    assert result["table"] == "processed"
    assert result["manifest"] == SAMPLE_SCHEMA_INFO["manifest"]
    assert result["crs"] == "EPSG:4326"
    assert result["run_id"] == "abc12345-def6-7890-abcd-ef1234567890"
    assert result["bounds"]["minx"] == -180.0
    assert result["bounds"]["miny"] == -90.0
    assert result["bounds"]["maxx"] == 180.0
    assert result["bounds"]["maxy"] == 90.0
    
    # Verify table existence check
    mock_postgis.table_exists.assert_called_once_with(
        "proc_abc12345_def6_7890_abcd_ef1234567890",
        "raw_data"
    )
    
    # Verify SQL execution (transform + index)
    assert mock_postgis.execute_sql.call_count == 2
    
    # Verify bounds calculation
    mock_postgis.get_table_bounds.assert_called_once_with(
        "proc_abc12345_def6_7890_abcd_ef1234567890",
        "processed"
    )


def test_spatial_transform_table_not_exists():
    """Test error handling when table doesn't exist."""
    mock_postgis = Mock()
    mock_postgis.table_exists.return_value = False
    
    mock_log = Mock()
    
    # Call should raise ValueError
    with pytest.raises(ValueError) as exc_info:
        _spatial_transform(
            postgis=mock_postgis,
            schema_info=SAMPLE_SCHEMA_INFO,
            log=mock_log,
        )
    
    assert "does not exist" in str(exc_info.value)
    assert "raw_data" in str(exc_info.value)


def test_spatial_transform_sql_execution():
    """Test that SQL is executed with correct schema."""
    mock_postgis = Mock()
    mock_postgis.table_exists.return_value = True
    mock_postgis.execute_sql = Mock()
    mock_postgis.get_table_bounds.return_value = Bounds(
        minx=-100.0,
        miny=-50.0,
        maxx=100.0,
        maxy=50.0,
    )
    
    mock_log = Mock()
    
    # Call core function
    _spatial_transform(
        postgis=mock_postgis,
        schema_info=SAMPLE_SCHEMA_INFO,
        log=mock_log,
    )
    
    # Verify execute_sql was called with correct schema
    calls = mock_postgis.execute_sql.call_args_list
    assert len(calls) == 2
    
    # First call: CREATE TABLE (transform)
    assert calls[0][0][1] == "proc_abc12345_def6_7890_abcd_ef1234567890"
    assert "CREATE TABLE processed" in calls[0][0][0]
    assert "ST_Transform" in calls[0][0][0]
    
    # Second call: CREATE INDEX
    assert calls[1][0][1] == "proc_abc12345_def6_7890_abcd_ef1234567890"
    assert "CREATE INDEX" in calls[1][0][0]
    assert "idx_processed_geom" in calls[1][0][0]


def test_spatial_transform_bounds_calculation():
    """Test bounds calculation."""
    mock_postgis = Mock()
    mock_postgis.table_exists.return_value = True
    mock_postgis.execute_sql = Mock()
    
    test_bounds = Bounds(
        minx=-120.0,
        miny=30.0,
        maxx=-110.0,
        maxy=40.0,
    )
    mock_postgis.get_table_bounds.return_value = test_bounds
    
    mock_log = Mock()
    
    # Call core function
    result = _spatial_transform(
        postgis=mock_postgis,
        schema_info=SAMPLE_SCHEMA_INFO,
        log=mock_log,
    )
    
    # Verify bounds in result
    assert result["bounds"]["minx"] == -120.0
    assert result["bounds"]["miny"] == 30.0
    assert result["bounds"]["maxx"] == -110.0
    assert result["bounds"]["maxy"] == 40.0


def test_spatial_transform_bounds_calculation_failure():
    """Test error handling when bounds calculation fails."""
    mock_postgis = Mock()
    mock_postgis.table_exists.return_value = True
    mock_postgis.execute_sql = Mock()
    mock_postgis.get_table_bounds.side_effect = RuntimeError("Bounds calculation failed")
    
    mock_log = Mock()
    
    # Call should raise RuntimeError
    with pytest.raises(RuntimeError) as exc_info:
        _spatial_transform(
            postgis=mock_postgis,
            schema_info=SAMPLE_SCHEMA_INFO,
            log=mock_log,
        )
    
    assert "Bounds calculation failed" in str(exc_info.value)


# =============================================================================
# Test: Dagster Op (spatial_transform)
# =============================================================================

def test_spatial_transform_op():
    """Test the Dagster op wrapper."""
    mock_postgis = Mock()
    mock_postgis.table_exists.return_value = True
    mock_postgis.execute_sql = Mock()
    mock_postgis.get_table_bounds.return_value = Bounds(
        minx=-180.0,
        miny=-90.0,
        maxx=180.0,
        maxy=90.0,
    )
    
    # Create mock context
    context = build_op_context(
        resources={"postgis": mock_postgis},
    )
    # Mock run_id property
    type(context).run_id = PropertyMock(return_value="abc12345-def6-7890-abcd-ef1234567890")
    
    # Call op
    result = spatial_transform(context, schema_info=SAMPLE_SCHEMA_INFO)
    
    # Verify result
    assert result["schema"] == "proc_abc12345_def6_7890_abcd_ef1234567890"
    assert result["table"] == "processed"
    assert result["crs"] == "EPSG:4326"

