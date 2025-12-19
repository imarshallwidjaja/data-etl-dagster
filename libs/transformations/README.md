# Transformations Library

A recipe-based transformation system that maps data processing intents to reusable, composable transformation steps.

## Overview

The transformation library provides a **registry pattern** for data transformations. Instead of writing transformation logic directly in pipeline code, you define reusable steps and compose them into recipes that are selected based on the manifest's `intent` field.

```
Manifest Intent  →  Recipe Registry  →  List of Steps  →  Executed in order
```

This approach enables:
- **Testability**: Steps can be unit tested in isolation
- **Reusability**: Steps are shared across recipes
- **Extensibility**: New intents just need new recipe mappings
- **Consistency**: Same intent always produces same transformation sequence

## Current Implementation

### Vector Transformations (PostGIS)

The current implementation focuses on spatial vector data using PostGIS as the compute engine:

```python
from libs.transformations import RecipeRegistry

# Get steps for an intent
steps = RecipeRegistry.get_vector_recipe("ingest_vector")

# Each step generates executable transformation logic
for step in steps:
    sql = step.generate_sql(schema, input_table, output_table)
    postgis.execute_sql(sql)
```

**Available Steps:**

| Step | What it does |
|------|--------------|
| `NormalizeCRSStep` | Transforms geometries to a target CRS (default: EPSG:4326) |
| `SimplifyGeometryStep` | Reduces geometry complexity while preserving topology |
| `CreateSpatialIndexStep` | Creates GIST spatial index for query performance |

**Current Recipes:**

| Intent | Steps Applied |
|--------|---------------|
| `ingest_vector` | Normalize CRS → Simplify → Create Index |
| `ingest_building_footprints` | Same, with stronger simplification |

Unknown intents fall back to the default recipe.

---

## Extending the System

### Adding a New Step

Steps are stateless classes that generate transformation logic:

```python
# vector.py
class ClipToBoundaryStep(VectorStep):
    def __init__(self, boundary_wkt: str, geom_column: str = "geom"):
        self.boundary_wkt = boundary_wkt
        self.geom_column = geom_column

    def generate_sql(self, schema: str, input_table: str, output_table: str) -> str:
        # Validate identifiers for safety
        _validate_identifier(schema, "schema")
        # ... return SQL that clips geometries to boundary
```

**Step contract:**
- Constructor accepts configuration parameters
- `generate_sql()` returns executable transformation logic
- Steps should be idempotent and deterministic

### Adding a New Recipe

Recipes are step compositions in `registry.py`:

```python
# registry.py
road_network_recipe = [
    NormalizeCRSStep(target_crs=4326),
    SimplifyGeometryStep(tolerance=0.00001),  # Less simplification for roads
    CreateSpatialIndexStep(),
]

recipes = {
    "ingest_vector": default_recipe,
    "ingest_road_network": road_network_recipe,  # New recipe
}
```

---

## Future Extensibility

The architecture is designed to support transformations beyond SQL:

### Non-SQL Steps

Future steps might operate on in-memory data:

```python
class PandasTransformStep(TransformStep):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Operate on DataFrame directly
        ...
```

### Different Registries

The pattern can extend to other data types:

```python
RecipeRegistry.get_raster_recipe(intent)   # Raster processing
RecipeRegistry.get_tabular_recipe(intent)  # Tabular transformations
```

### Step Composition Patterns

Future recipes might include:
- **Conditional steps**: Apply based on data characteristics
- **Parallel steps**: Independent steps run concurrently
- **Branching recipes**: Different paths based on intermediate results

---

## Design Principles

1. **Separation of concerns**: Transformation logic lives here, not in Dagster ops
2. **Configuration over code**: New intents = new mappings, not new code paths
3. **Fail fast**: Validate identifiers and inputs before generating SQL
4. **Single geometry column**: Vector steps assume one geometry column named `geom`

---

## Testing

```bash
# Unit tests for registry and steps
pytest tests/unit/libs/test_registry.py
pytest tests/unit/libs/test_vector_steps.py
pytest tests/unit/libs/test_transformations_base.py

# Integration tests (requires Docker stack)
pytest -m integration tests/integration/
```

---

## Files

| File | Purpose |
|------|---------|
| `registry.py` | Intent → recipe mapping |
| `vector.py` | Spatial transformation steps |
| `base.py` | Abstract step interfaces |
