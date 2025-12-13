# =============================================================================
# Unit Tests: Vector Transformation Steps
# =============================================================================

import pytest

from libs.transformations import (
    NormalizeCRSStep,
    SimplifyGeometryStep,
    CreateSpatialIndexStep,
)


# =============================================================================
# Test: NormalizeCRSStep
# =============================================================================

def test_normalize_crs_step_default_target_crs():
    """Test that default target_crs is 4326."""
    step = NormalizeCRSStep()
    assert step.target_crs == 4326


def test_normalize_crs_step_custom_target_crs():
    """Test that custom target_crs is used."""
    step = NormalizeCRSStep(target_crs=3857)
    assert step.target_crs == 3857


def test_normalize_crs_step_generates_correct_sql():
    """Test that SQL generation includes ST_Transform with correct CRS."""
    step = NormalizeCRSStep()
    sql = step.generate_sql("proc_test", "raw_data", "step_0")
    
    assert "CREATE TABLE step_0" in sql
    assert "ST_Transform(geom, 4326)" in sql
    assert "FROM raw_data" in sql
    assert "as geom" in sql


def test_normalize_crs_step_custom_crs_in_sql():
    """Test that custom CRS is used in SQL generation."""
    step = NormalizeCRSStep(target_crs=3857)
    sql = step.generate_sql("proc_test", "raw_data", "step_0")
    
    assert "ST_Transform(geom, 3857)" in sql


def test_normalize_crs_step_table_names_interpolated():
    """Test that table names are correctly interpolated in SQL."""
    step = NormalizeCRSStep()
    sql = step.generate_sql("proc_test", "input_table", "output_table")
    
    assert "FROM input_table" in sql
    assert "CREATE TABLE output_table" in sql


def test_normalize_crs_step_schema_parameter_accepted():
    """Test that schema parameter is accepted (even if not used)."""
    step = NormalizeCRSStep()
    sql = step.generate_sql("proc_test_schema", "raw_data", "step_0")
    
    # SQL should be generated successfully
    assert isinstance(sql, str)
    assert len(sql) > 0


# =============================================================================
# Test: SimplifyGeometryStep
# =============================================================================

def test_simplify_geometry_step_default_tolerance():
    """Test that default tolerance is 0.0001."""
    step = SimplifyGeometryStep()
    assert step.tolerance == 0.0001


def test_simplify_geometry_step_custom_tolerance():
    """Test that custom tolerance is used."""
    step = SimplifyGeometryStep(tolerance=0.001)
    assert step.tolerance == 0.001


def test_simplify_geometry_step_generates_correct_sql():
    """Test that SQL generation includes ST_SimplifyPreserveTopology."""
    step = SimplifyGeometryStep()
    sql = step.generate_sql("proc_test", "step_0", "step_1")
    
    assert "CREATE TABLE step_1" in sql
    assert "ST_SimplifyPreserveTopology(geom, 0.0001)" in sql
    assert "FROM step_0" in sql
    assert "as geom_simple" in sql


def test_simplify_geometry_step_custom_tolerance_in_sql():
    """Test that custom tolerance is used in SQL generation."""
    step = SimplifyGeometryStep(tolerance=0.001)
    sql = step.generate_sql("proc_test", "step_0", "step_1")
    
    assert "ST_SimplifyPreserveTopology(geom, 0.001)" in sql


def test_simplify_geometry_step_table_names_interpolated():
    """Test that table names are correctly interpolated in SQL."""
    step = SimplifyGeometryStep()
    sql = step.generate_sql("proc_test", "input_table", "output_table")
    
    assert "FROM input_table" in sql
    assert "CREATE TABLE output_table" in sql


# =============================================================================
# Test: CreateSpatialIndexStep
# =============================================================================

def test_create_spatial_index_step_generates_correct_sql():
    """Test that SQL generation includes CREATE INDEX with GIST."""
    step = CreateSpatialIndexStep()
    sql = step.generate_sql("proc_test", "processed", "ignored")
    
    assert "CREATE INDEX" in sql
    assert "USING GIST" in sql
    assert "idx_processed_geom" in sql
    assert "ON processed" in sql
    assert "(geom)" in sql


def test_create_spatial_index_step_index_name_includes_table():
    """Test that index name includes table name."""
    step = CreateSpatialIndexStep()
    sql = step.generate_sql("proc_test", "step_1", "ignored")
    
    assert "idx_step_1_geom" in sql
    assert "ON step_1" in sql


def test_create_spatial_index_step_output_table_ignored():
    """Test that output_table parameter is ignored for index steps."""
    step = CreateSpatialIndexStep()
    sql1 = step.generate_sql("proc_test", "processed", "ignored_output")
    sql2 = step.generate_sql("proc_test", "processed", "different_output")
    
    # Both should generate identical SQL (output_table is ignored)
    assert sql1 == sql2
    assert "ON processed" in sql1
    assert "ON processed" in sql2

