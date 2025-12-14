# Library Context: Transformations

## Overview

This library provides a recipe-based transformation architecture for the spatial ETL pipeline. It enables flexible, intent-driven spatial transformations by organizing SQL operations into reusable step classes.

## Responsibilities

1. **Step Classes**: Define individual transformation operations (CRS normalization, geometry simplification, indexing, etc.)
2. **Recipe Registry**: Map manifest intent fields to lists of transformation steps
3. **SQL Generation**: Generate PostGIS SQL statements for each transformation step
4. **Table Chaining**: Support intermediate table creation for debugging and step isolation

## Module Structure

```
libs/transformations/
├── CONTEXT.md           # This file
├── __init__.py          # Package exports
├── base.py              # Base classes (TransformStep, VectorStep)
├── vector.py            # Vector transformation steps
└── registry.py          # RecipeRegistry class
```

## Input/Output Contracts

### TransformStep Interface

All transformation steps implement the `TransformStep` abstract base class:

```python
class TransformStep(ABC):
    @abstractmethod
    def generate_sql(self, schema: str, input_table: str, output_table: str) -> str:
        """Generate SQL for this transformation step."""
        pass
```

**Parameters:**
- `schema`: Schema name (for potential future use, currently not needed since `execute_sql` sets `search_path`)
- `input_table`: Input table name (unqualified, search_path is set)
- `output_table`: Output table name (unqualified, search_path is set)

**Returns:**
- SQL string to execute via `PostGISResource.execute_sql`

### Geometry Column Contract

All vector transformation steps enforce a **single-geometry-column** contract:

- **Input Assumption**: Input table has exactly one geometry column named `geom_column` (default: `"geom"`)
- **Output Guarantee**: Output table has exactly one geometry column named `geom_column` containing the transformed geometry
- **Fail-Fast Validation**: Steps validate the contract at runtime using `information_schema.columns`
- **Overwrite Semantics**: Geometry steps overwrite the geometry column rather than creating additional columns

**Benefits:**
- Predictable geometry column naming throughout the pipeline
- Prevention of geometry column proliferation
- Clear contract for downstream operations
- Runtime validation prevents silent failures

### Vector Steps

**NormalizeCRSStep:**
- Transforms geometries to target CRS (default: EPSG:4326)
- Creates new table with transformed geometry (overwrites geometry column)
- Parameters: `target_crs: int = 4326`, `geom_column: str = "geom"`

**SimplifyGeometryStep:**
- Simplifies geometries while preserving topology
- Creates new table with simplified geometry (overwrites geometry column)
- Parameters: `tolerance: float = 0.0001`, `geom_column: str = "geom"`

**CreateSpatialIndexStep:**
- Creates GIST spatial index on geometry column
- Operates on existing table (does not create new table)
- Parameters: `geom_column: str = "geom"`

### RecipeRegistry

**get_vector_recipe(intent: str, geom_column: str = "geom") -> List[VectorStep]:**
- Maps manifest intent to list of transformation steps
- Returns default recipe for unknown intents (backward compatibility)
- Steps are instantiated fresh each time (no shared state)
- All steps configured with specified geometry column

## Design Principles

### SQL Safety

- Table names come from trusted sources (internal step chaining)
- Schema names already sanitized via `RunIdSchemaMapping`
- SQL generation uses f-strings for table name interpolation (safe for PostgreSQL identifiers)

### Table Chaining Pattern

Steps create intermediate tables (`step_0`, `step_1`, etc.) that are chained together:
- `raw_data` → `step_0` → `step_1` → `processed`

**Benefits:**
- Debugging: Can inspect intermediate states
- Step isolation: Each step operates independently
- Easy rollback: Drop intermediate tables if needed

### Step Types

**Transform Steps:**
- Create new tables via `CREATE TABLE ... AS SELECT`
- Examples: `NormalizeCRSStep`, `SimplifyGeometryStep`

**Index Steps:**
- Operate on existing tables without creating new ones
- Examples: `CreateSpatialIndexStep`

### Schema Context

All SQL generation assumes `execute_sql` sets `search_path` to the schema:
- Table references don't need schema prefix in SELECT/FROM clauses
- CREATE TABLE statements operate within schema context
- Index creation operates on tables within the schema context

## Usage Examples

### Using Recipe Registry

```python
from libs.transformations import RecipeRegistry

# Get recipe for manifest intent
intent = manifest.get("intent", "ingest_vector")
recipe = RecipeRegistry.get_vector_recipe(intent)

# Execute steps with table chaining
current_table = "raw_data"
for i, step in enumerate(recipe):
    if isinstance(step, CreateSpatialIndexStep):
        # Index steps don't create new tables
        sql = step.generate_sql(schema, current_table, current_table)
        postgis.execute_sql(sql, schema)
    else:
        next_table = f"step_{i}"
        sql = step.generate_sql(schema, current_table, next_table)
        postgis.execute_sql(sql, schema)
        current_table = next_table

# Rename final table
rename_sql = f'ALTER TABLE {current_table} RENAME TO "processed";'
postgis.execute_sql(rename_sql, schema)
```

### Creating Custom Steps

```python
from libs.transformations import VectorStep

class CustomTransformStep(VectorStep):
    def generate_sql(self, schema: str, input_table: str, output_table: str) -> str:
        return f"""
        CREATE TABLE {output_table} AS
        SELECT *, ST_Buffer(geom, 100) as geom_buffered
        FROM {input_table};
        """
```

## Relation to Global Architecture

```
┌──────────────────────────────────────────────────────────────┐
│              Transformations (This Library)                   │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│   Dagster transform_op.py                                    │
│         │                                                     │
│         ▼                                                     │
│   ┌──────────────────────┐                                  │
│   │ RecipeRegistry        │───► [Step1, Step2, Step3]        │
│   │ get_vector_recipe()  │                                  │
│   └──────────────────────┘                                  │
│         │                                                     │
│         ▼                                                     │
│   Execute steps with table chaining                           │
│         │                                                     │
│         ▼                                                     │
│   PostGISResource.execute_sql()                              │
│         │                                                     │
│         ▼                                                     │
│   PostGIS Ephemeral Schema                                   │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

**Integration Points:**
- Used by `services/dagster/etl_pipelines/ops/transform_op.py`
- Reads manifest `intent` field to select recipe
- Generates SQL executed via `PostGISResource.execute_sql`
- Operates within ephemeral schema context (`proc_{run_id}`)

## Configuration Requirements

### Package Installation

The transformations library is part of the `spatial-etl-libs` package:
- Installed via `pip install -e ./libs` (development)
- Installed in Docker `user-code` container during build
- Changes require rebuilding container: `docker-compose build user-code`

### Dependencies

- No external dependencies beyond standard library and `abc` module
- Compatible with Python 3.10+

## Development Notes

- Steps are stateless (no instance variables beyond constructor parameters)
- Registry returns new step instances each time (no shared state)
- SQL generation is deterministic (same inputs = same SQL)
- Steps can be tested independently (mock `generate_sql` return values)

### Testing Strategy

**Unit Tests:**
- Test SQL generation for each step
- Test registry returns correct recipes for intents
- Test step instantiation and parameter defaults

**Integration Tests:**
- Test full recipe execution against real PostGIS
- Verify table chaining works correctly
- Verify intermediate tables are created and cleaned up

## Future Enhancements

- Raster transformation recipes
- User-configurable recipe parameters
- Recipe validation and schema checking
- Recipe versioning
- Custom step types beyond vector
- Recipe composition (nested recipes)
- Intent-specific recipe customization

## Common Mistakes

- **Forgetting to handle index steps**: Index steps don't create new tables, must be handled differently
- **Not renaming final table**: Must rename final step table to `processed` for downstream ops
- **Shared step instances**: Registry creates new instances each time, don't cache steps
- **SQL injection concerns**: Table names come from trusted sources, but still validate identifiers

