# =============================================================================
# Unit Tests: Recipe Registry
# =============================================================================

import pytest
import pyarrow as pa

from libs.transformations import (
    RecipeRegistry,
    NormalizeCRSStep,
    SimplifyGeometryStep,
    CreateSpatialIndexStep,
)


# =============================================================================
# Test: get_vector_recipe
# =============================================================================


def test_registry_returns_default_recipe_for_unknown_intent():
    """Test that registry returns default recipe for unknown intent."""
    recipe = RecipeRegistry.get_vector_recipe("unknown_intent")

    assert len(recipe) == 3
    assert isinstance(recipe[0], NormalizeCRSStep)
    assert isinstance(recipe[1], SimplifyGeometryStep)
    assert isinstance(recipe[2], CreateSpatialIndexStep)


def test_registry_returns_correct_recipe_for_ingest_vector():
    """Test that registry returns correct recipe for ingest_vector intent."""
    recipe = RecipeRegistry.get_vector_recipe("ingest_vector")

    assert len(recipe) == 3
    assert isinstance(recipe[0], NormalizeCRSStep)
    assert isinstance(recipe[1], SimplifyGeometryStep)
    assert isinstance(recipe[2], CreateSpatialIndexStep)


def test_registry_returns_correct_recipe_for_ingest_road_network():
    """Test that registry returns correct recipe for ingest_road_network intent."""
    recipe = RecipeRegistry.get_vector_recipe("ingest_road_network")

    assert len(recipe) == 3
    assert isinstance(recipe[0], NormalizeCRSStep)
    assert isinstance(recipe[1], SimplifyGeometryStep)
    assert isinstance(recipe[2], CreateSpatialIndexStep)


def test_registry_returns_steps_in_correct_order():
    """Test that registry returns steps in the correct order."""
    recipe = RecipeRegistry.get_vector_recipe("ingest_vector")

    # Verify order: NormalizeCRSStep -> SimplifyGeometryStep -> CreateSpatialIndexStep
    assert isinstance(recipe[0], NormalizeCRSStep)
    assert isinstance(recipe[1], SimplifyGeometryStep)
    assert isinstance(recipe[2], CreateSpatialIndexStep)


def test_registry_returns_new_instances():
    """Test that registry returns new step instances (not shared)."""
    recipe1 = RecipeRegistry.get_vector_recipe("ingest_vector")
    recipe2 = RecipeRegistry.get_vector_recipe("ingest_vector")

    # Steps should be different instances
    assert recipe1[0] is not recipe2[0]
    assert recipe1[1] is not recipe2[1]
    assert recipe1[2] is not recipe2[2]

    # But should be same type
    assert type(recipe1[0]) == type(recipe2[0])
    assert type(recipe1[1]) == type(recipe2[1])
    assert type(recipe1[2]) == type(recipe2[2])


def test_registry_default_recipe_matches_known_intents():
    """Test that default recipe matches known intent recipes."""
    default_recipe = RecipeRegistry.get_vector_recipe("unknown")
    ingest_vector_recipe = RecipeRegistry.get_vector_recipe("ingest_vector")

    # Should have same structure
    assert len(default_recipe) == len(ingest_vector_recipe)
    assert type(default_recipe[0]) == type(ingest_vector_recipe[0])
    assert type(default_recipe[1]) == type(ingest_vector_recipe[1])
    assert type(default_recipe[2]) == type(ingest_vector_recipe[2])


# =============================================================================
# Test: get_tabular_reader
# =============================================================================


def test_registry_get_tabular_reader_csv_case_insensitive():
    """CSV format resolves to CSV reader regardless of case."""
    reader_upper = RecipeRegistry.get_tabular_reader("CSV")
    reader_mixed = RecipeRegistry.get_tabular_reader("cSv")

    assert callable(reader_upper)
    assert callable(reader_mixed)
    assert reader_upper is reader_mixed


def test_registry_get_tabular_reader_parquet_case_insensitive():
    """Parquet format resolves to Parquet reader regardless of case."""
    reader_title = RecipeRegistry.get_tabular_reader("Parquet")
    reader_lower = RecipeRegistry.get_tabular_reader("parquet")

    assert callable(reader_title)
    assert callable(reader_lower)
    assert reader_title is reader_lower


def test_registry_get_tabular_reader_rejects_unknown_format():
    """Unknown tabular formats raise a clear error."""
    with pytest.raises(ValueError, match="Unsupported tabular format"):
        RecipeRegistry.get_tabular_reader("xlsx")


def test_registry_get_tabular_reader_rejects_empty_format():
    """Empty or whitespace format values are rejected."""
    with pytest.raises(ValueError, match="Unsupported tabular format"):
        RecipeRegistry.get_tabular_reader("   ")


def test_registry_csv_reader_reads_arrow_table(tmp_path):
    """CSV reader returns pyarrow.Table from CSV input."""
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("id,name\n1,Alice\n")

    reader = RecipeRegistry.get_tabular_reader("CSV")
    table = reader(str(csv_path))

    assert isinstance(table, pa.Table)
    assert table.column_names == ["id", "name"]
    assert table.num_rows == 1


def test_registry_parquet_reader_reads_arrow_table(tmp_path):
    """Parquet reader returns pyarrow.Table from parquet input."""
    parquet_path = tmp_path / "sample.parquet"
    table_in = pa.table({"id": ["1"], "name": ["Alice"]})
    import pyarrow.parquet as pq

    pq.write_table(table_in, parquet_path)

    reader = RecipeRegistry.get_tabular_reader("Parquet")
    table_out = reader(str(parquet_path))

    assert isinstance(table_out, pa.Table)
    assert table_out.column_names == ["id", "name"]
    assert table_out.num_rows == 1
