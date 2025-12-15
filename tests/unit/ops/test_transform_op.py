# =============================================================================
# Unit Tests: Transform Op
# =============================================================================

import pytest
from unittest.mock import Mock, patch, PropertyMock
from dagster import build_op_context

from services.dagster.etl_pipelines.ops.transform_op import spatial_transform, _spatial_transform
from libs.models import Bounds
from libs.transformations import NormalizeCRSStep, SimplifyGeometryStep, CreateSpatialIndexStep


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
            "format": "GeoJSON"
            }
        ],
        "metadata": {
            "project": "TEST_PROJECT",
        "description": "Test dataset",
        "tags": {},
        "join_config": None,
        }
    },
    "tables": ["raw_data"],
    "run_id": "abc12345-def6-7890-abcd-ef1234567890",
    "geom_column": "geom",
}


# =============================================================================
# Test: Core Logic (_spatial_transform)
# =============================================================================

@patch('services.dagster.etl_pipelines.ops.transform_op.RecipeRegistry')
def test_spatial_transform_success(mock_registry):
    """Test successful spatial transformation."""
    # Create mock recipe steps
    mock_step1 = NormalizeCRSStep()
    mock_step2 = SimplifyGeometryStep()
    mock_step3 = CreateSpatialIndexStep()
    mock_registry.get_vector_recipe.return_value = [mock_step1, mock_step2, mock_step3]
    
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
    
    # Verify registry was called with correct intent and geom_column
    mock_registry.get_vector_recipe.assert_called_once_with("ingest_vector", geom_column="geom")
    
    # Verify table existence check
    mock_postgis.table_exists.assert_called_once_with(
        "proc_abc12345_def6_7890_abcd_ef1234567890",
        "raw_data"
    )
    
    # Verify SQL execution (2 transform steps + 1 index step + 1 rename = 4 calls)
    assert mock_postgis.execute_sql.call_count == 4
    
    # Verify bounds calculation
    mock_postgis.get_table_bounds.assert_called_once_with(
        "proc_abc12345_def6_7890_abcd_ef1234567890",
        "processed",
        geom_column="geom"
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


@patch('services.dagster.etl_pipelines.ops.transform_op.RecipeRegistry')
def test_spatial_transform_sql_execution(mock_registry):
    """Test that SQL is executed with correct schema and table chaining."""
    # Create mock recipe steps
    mock_step1 = NormalizeCRSStep()
    mock_step2 = SimplifyGeometryStep()
    mock_step3 = CreateSpatialIndexStep()
    mock_registry.get_vector_recipe.return_value = [mock_step1, mock_step2, mock_step3]
    
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
    
    # Verify execute_sql was called with correct schema for all calls
    calls = mock_postgis.execute_sql.call_args_list
    assert len(calls) == 4  # 2 transform steps + 1 index step + 1 rename
    
    # All calls should use the same schema
    schema = "proc_abc12345_def6_7890_abcd_ef1234567890"
    for call in calls:
        assert call[0][1] == schema
    
    # Verify table chaining: step_0, step_1, then rename to processed
    # First call: NormalizeCRSStep creates step_0
    assert "step_0" in calls[0][0][0]
    assert "raw_data" in calls[0][0][0]
    
    # Second call: SimplifyGeometryStep creates step_1
    assert "step_1" in calls[1][0][0]
    assert "step_0" in calls[1][0][0]
    
    # Third call: CreateSpatialIndexStep creates index on step_1
    assert "step_1" in calls[2][0][0]
    assert "CREATE INDEX" in calls[2][0][0]
    
    # Fourth call: Rename step_1 to processed
    assert "RENAME TO" in calls[3][0][0]
    assert "processed" in calls[3][0][0]


@patch('services.dagster.etl_pipelines.ops.transform_op.RecipeRegistry')
def test_spatial_transform_bounds_calculation(mock_registry):
    """Test bounds calculation."""
    # Create mock recipe steps
    mock_step1 = NormalizeCRSStep()
    mock_step2 = SimplifyGeometryStep()
    mock_step3 = CreateSpatialIndexStep()
    mock_registry.get_vector_recipe.return_value = [mock_step1, mock_step2, mock_step3]

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


@patch('services.dagster.etl_pipelines.ops.transform_op.RecipeRegistry')
def test_spatial_transform_bounds_calculation_none(mock_registry):
    """Test bounds calculation when bounds are None (empty geometry)."""
    # Create mock recipe steps
    mock_step1 = NormalizeCRSStep()
    mock_step2 = SimplifyGeometryStep()
    mock_step3 = CreateSpatialIndexStep()
    mock_registry.get_vector_recipe.return_value = [mock_step1, mock_step2, mock_step3]

    mock_postgis = Mock()
    mock_postgis.table_exists.return_value = True
    mock_postgis.execute_sql = Mock()

    # Return None for empty geometry
    mock_postgis.get_table_bounds.return_value = None

    mock_log = Mock()

    # Call core function
    result = _spatial_transform(
        postgis=mock_postgis,
        schema_info=SAMPLE_SCHEMA_INFO,
        log=mock_log,
    )

    # Verify bounds in result is None
    assert result["bounds"] is None


@patch('services.dagster.etl_pipelines.ops.transform_op.RecipeRegistry')
def test_spatial_transform_bounds_calculation_failure(mock_registry):
    """Test error handling when bounds calculation fails."""
    # Create mock recipe steps
    mock_step1 = NormalizeCRSStep()
    mock_step2 = SimplifyGeometryStep()
    mock_step3 = CreateSpatialIndexStep()
    mock_registry.get_vector_recipe.return_value = [mock_step1, mock_step2, mock_step3]
    
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

@patch('services.dagster.etl_pipelines.ops.transform_op.RecipeRegistry')
def test_spatial_transform_op(mock_registry):
    """Test the Dagster op wrapper."""
    # Create mock recipe steps
    mock_step1 = NormalizeCRSStep()
    mock_step2 = SimplifyGeometryStep()
    mock_step3 = CreateSpatialIndexStep()
    mock_registry.get_vector_recipe.return_value = [mock_step1, mock_step2, mock_step3]
    
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


# =============================================================================
# Test: Recipe Registry Integration
# =============================================================================

@patch('services.dagster.etl_pipelines.ops.transform_op.RecipeRegistry')
def test_spatial_transform_uses_registry(mock_registry):
    """Test that registry is called with intent from manifest."""
    # Create mock recipe steps
    mock_step1 = Mock()
    mock_step1.generate_sql.return_value = "CREATE TABLE step_0 AS SELECT * FROM raw_data;"
    mock_step2 = Mock()
    mock_step2.generate_sql.return_value = "CREATE TABLE step_1 AS SELECT * FROM step_0;"
    mock_registry.get_vector_recipe.return_value = [mock_step1, mock_step2]
    
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
    _spatial_transform(
        postgis=mock_postgis,
        schema_info=SAMPLE_SCHEMA_INFO,
        log=mock_log,
    )
    
    # Verify registry called with correct intent and geom_column
    mock_registry.get_vector_recipe.assert_called_once_with("ingest_vector", geom_column="geom")
    
    # Verify steps executed in order
    assert mock_postgis.execute_sql.call_count == 3  # 2 steps + rename


@patch('services.dagster.etl_pipelines.ops.transform_op.RecipeRegistry')
def test_spatial_transform_handles_index_steps(mock_registry):
    """Test that index steps don't create new tables."""
    # Create mock recipe with index step
    mock_transform_step = Mock()
    mock_transform_step.generate_sql.return_value = "CREATE TABLE step_0 AS SELECT * FROM raw_data;"
    mock_index_step = CreateSpatialIndexStep()
    mock_registry.get_vector_recipe.return_value = [mock_transform_step, mock_index_step]
    
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
    _spatial_transform(
        postgis=mock_postgis,
        schema_info=SAMPLE_SCHEMA_INFO,
        log=mock_log,
    )
    
    # Verify execute_sql called 3 times (1 transform + 1 index + 1 rename)
    assert mock_postgis.execute_sql.call_count == 3
    
    # Verify index step SQL uses step_0 (not a new table)
    calls = mock_postgis.execute_sql.call_args_list
    index_call = calls[1]  # Second call is index
    assert "step_0" in index_call[0][0]
    assert "CREATE INDEX" in index_call[0][0]


@patch('services.dagster.etl_pipelines.ops.transform_op.RecipeRegistry')
def test_spatial_transform_table_chaining(mock_registry):
    """Test that intermediate tables are created correctly in chain."""
    # Create mock recipe steps
    mock_step1 = NormalizeCRSStep()
    mock_step2 = SimplifyGeometryStep()
    mock_step3 = CreateSpatialIndexStep()
    mock_registry.get_vector_recipe.return_value = [mock_step1, mock_step2, mock_step3]
    
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
    _spatial_transform(
        postgis=mock_postgis,
        schema_info=SAMPLE_SCHEMA_INFO,
        log=mock_log,
    )
    
    # Verify table chaining: raw_data -> step_0 -> step_1 -> processed
    calls = mock_postgis.execute_sql.call_args_list
    
    # First call: NormalizeCRSStep creates step_0 from raw_data
    assert "step_0" in calls[0][0][0]
    assert "raw_data" in calls[0][0][0]
    
    # Second call: SimplifyGeometryStep creates step_1 from step_0
    assert "step_1" in calls[1][0][0]
    assert "step_0" in calls[1][0][0]
    
    # Third call: CreateSpatialIndexStep creates index on step_1
    assert "step_1" in calls[2][0][0]
    assert "CREATE INDEX" in calls[2][0][0]
    
    # Fourth call: Rename step_1 to processed
    assert "RENAME TO" in calls[3][0][0]
    assert "processed" in calls[3][0][0]

