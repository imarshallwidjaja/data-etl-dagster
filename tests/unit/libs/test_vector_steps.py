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
    assert step.geom_column == "geom"


def test_normalize_crs_step_custom_target_crs():
    """Test that custom target_crs is used."""
    step = NormalizeCRSStep(target_crs=3857)
    assert step.target_crs == 3857
    assert step.geom_column == "geom"


def test_normalize_crs_step_generates_correct_sql():
    """Test that SQL generation includes ST_Transform with correct CRS."""
    step = NormalizeCRSStep()
    sql = step.generate_sql("proc_test", "raw_data", "step_0")

    # Uses DO $$ ... $$; single-statement format
    assert "DO $$" in sql
    assert "EXECUTE format" in sql
    assert "ST_Transform" in sql
    assert "4326" in sql  # EPSG code as format argument
    assert "'step_0'" in sql  # output table as format argument
    assert "'raw_data'" in sql  # input table as format argument
    # Should NOT include SELECT * (uses explicit column list)
    assert "SELECT *" not in sql
    # Should NOT include "as geom" (overwrites geometry column)
    assert "as geom" not in sql


def test_normalize_crs_step_custom_crs_in_sql():
    """Test that custom CRS is used in SQL generation."""
    step = NormalizeCRSStep(target_crs=3857)
    sql = step.generate_sql("proc_test", "raw_data", "step_0")

    assert "ST_Transform" in sql
    assert "3857" in sql  # Custom EPSG code as format argument


def test_normalize_crs_step_table_names_interpolated():
    """Test that table names are correctly interpolated in SQL."""
    step = NormalizeCRSStep()
    sql = step.generate_sql("proc_test", "input_table", "output_table")

    assert "'input_table'" in sql  # input table as format argument
    assert "'output_table'" in sql  # output table as format argument


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
    assert step.geom_column == "geom"


def test_simplify_geometry_step_custom_tolerance():
    """Test that custom tolerance is used."""
    step = SimplifyGeometryStep(tolerance=0.001)
    assert step.tolerance == 0.001
    assert step.geom_column == "geom"


def test_simplify_geometry_step_generates_correct_sql():
    """Test that SQL generation includes ST_SimplifyPreserveTopology."""
    step = SimplifyGeometryStep()
    sql = step.generate_sql("proc_test", "step_0", "step_1")

    # Uses DO $$ ... $$; single-statement format
    assert "DO $$" in sql
    assert "EXECUTE format" in sql
    assert "ST_SimplifyPreserveTopology" in sql
    assert "0.0001" in sql  # tolerance as format argument
    assert "'step_1'" in sql  # output table as format argument
    assert "'step_0'" in sql  # input table as format argument
    # Should NOT include geom_simple (overwrites geometry column)
    assert "geom_simple" not in sql
    # Should NOT include SELECT * (uses explicit column list)
    assert "SELECT *" not in sql


def test_simplify_geometry_step_custom_tolerance_in_sql():
    """Test that custom tolerance is used in SQL generation."""
    step = SimplifyGeometryStep(tolerance=0.001)
    sql = step.generate_sql("proc_test", "step_0", "step_1")

    assert "ST_SimplifyPreserveTopology" in sql
    assert "0.001" in sql  # Custom tolerance as format argument


def test_simplify_geometry_step_table_names_interpolated():
    """Test that table names are correctly interpolated in SQL."""
    step = SimplifyGeometryStep()
    sql = step.generate_sql("proc_test", "input_table", "output_table")

    assert "'input_table'" in sql  # input table as format argument
    assert "'output_table'" in sql  # output table as format argument


# =============================================================================
# Test: CreateSpatialIndexStep
# =============================================================================

def test_create_spatial_index_step_generates_correct_sql():
    """Test that SQL generation includes CREATE INDEX with GIST."""
    step = CreateSpatialIndexStep()
    sql = step.generate_sql("proc_test", "processed", "ignored")

    # Uses DO $$ ... $$; single-statement format
    assert "DO $$" in sql
    assert "EXECUTE format" in sql
    assert "CREATE INDEX idx_%I_%I" in sql
    assert "USING GIST" in sql
    assert "'processed'" in sql  # table name as format argument
    assert "'geom'" in sql  # geometry column as format argument


def test_create_spatial_index_step_index_name_includes_table():
    """Test that index name includes table name."""
    step = CreateSpatialIndexStep()
    sql = step.generate_sql("proc_test", "step_1", "ignored")

    assert "'step_1'" in sql  # table name as format argument
    assert "'geom'" in sql  # geometry column as format argument


def test_create_spatial_index_step_output_table_ignored():
    """Test that output_table parameter is ignored for index steps."""
    step = CreateSpatialIndexStep()
    sql1 = step.generate_sql("proc_test", "processed", "ignored_output")
    sql2 = step.generate_sql("proc_test", "processed", "different_output")

    # Both should generate identical SQL (output_table is ignored)
    assert sql1 == sql2
    assert "'processed'" in sql1  # table name as format argument
    assert "'processed'" in sql2


def test_normalize_crs_step_geom_column_parameter():
    """Test that geom_column parameter is used."""
    step = NormalizeCRSStep(geom_column="geometry")
    assert step.geom_column == "geometry"

    sql = step.generate_sql("proc_test", "raw_data", "step_0")
    assert "ST_Transform" in sql
    assert "column_name = 'geometry'" in sql  # validation check
    assert "'geometry'" in sql  # format argument


def test_simplify_geometry_step_geom_column_parameter():
    """Test that geom_column parameter is used."""
    step = SimplifyGeometryStep(geom_column="geometry")
    assert step.geom_column == "geometry"

    sql = step.generate_sql("proc_test", "step_0", "step_1")
    assert "ST_SimplifyPreserveTopology" in sql
    assert "column_name = 'geometry'" in sql  # validation check
    assert "'geometry'" in sql  # format argument


def test_create_spatial_index_step_geom_column_parameter():
    """Test that geom_column parameter is used."""
    step = CreateSpatialIndexStep(geom_column="geometry")
    assert step.geom_column == "geometry"

    sql = step.generate_sql("proc_test", "processed", "ignored")
    assert "column_name = 'geometry'" in sql  # validation check
    assert "'geometry'" in sql  # format argument


def test_normalize_crs_step_fail_fast_precondition():
    """Test that SQL contains fail-fast precondition for geometry column validation."""
    step = NormalizeCRSStep()
    sql = step.generate_sql("proc_test", "raw_data", "step_0")

    assert "RAISE EXCEPTION" in sql
    assert "exactly one geometry column" in sql
    assert "geometry column" in sql
    assert "not found" in sql


def test_simplify_geometry_step_fail_fast_precondition():
    """Test that SQL contains fail-fast precondition for geometry column validation."""
    step = SimplifyGeometryStep()
    sql = step.generate_sql("proc_test", "step_0", "step_1")

    assert "RAISE EXCEPTION" in sql
    assert "exactly one geometry column" in sql
    assert "geometry column" in sql
    assert "not found" in sql


def test_create_spatial_index_step_fail_fast_precondition():
    """Test that SQL contains fail-fast precondition for geometry column validation."""
    step = CreateSpatialIndexStep()
    sql = step.generate_sql("proc_test", "processed", "ignored")

    assert "RAISE EXCEPTION" in sql
    assert "exactly one geometry column" in sql
    assert "geometry column" in sql
    assert "not found" in sql

