# services/postgis/ — Agent Guide

## What this directory is / owns

This directory owns PostGIS initialization and documents its role as **transient compute**.
All persistent data must live in MinIO (files) + MongoDB (ledger).

## Key invariants / non-negotiables

- **Never persist** durable datasets in PostGIS.
- **Ephemeral schemas per run**: create → use → drop (even on failure).
- **Geometry column**: standardized to `geom` in all compute schemas.

## Entry points / key files

- `init/01-init-extensions.sql`: PostGIS initialization script (extensions + utility functions)
- Dev configuration: `../../docker-compose.yaml`

### Extensions enabled

- `postgis`, `postgis_topology`, `postgis_raster`: spatial operations
- `fuzzystrmatch`: fuzzy string matching
- `uuid-ossp`: UUID generation

### Utility functions

| Function | Description |
|----------|-------------|
| `create_processing_schema(run_id)` | Creates `proc_{run_id}` schema with proper permissions |
| `drop_processing_schema(run_id)` | Drops schema with CASCADE (for cleanup) |
| `list_processing_schemas()` | Lists all `proc_%` schemas (monitoring) |
| `cleanup_all_processing_schemas()` | Emergency cleanup - drops ALL processing schemas |

## How to work here

- The pipeline creates schemas like `proc_<run_id_sanitized>` and drops them after export.
- Schema naming: `proc_{run_id}` with hyphens replaced by underscores.
- The `drop_processing_schema()` function is called in the cleanup op's `finally` block.

## Common tasks

- **Add an extension / init SQL**: add it to `init/01-init-extensions.sql` and verify in integration tests.
- **Investigate leaked schemas** (dev): 
  ```sql
  SELECT * FROM list_processing_schemas();
  -- or emergency cleanup:
  SELECT cleanup_all_processing_schemas();
  ```

## Testing / verification

- Connectivity: `pytest -m integration tests/integration/test_postgis.py`
- Initialization: `pytest -m integration tests/integration/test_postgis_init.py`
- Schema lifecycle: `pytest -m integration tests/integration/test_schema_cleanup.py`

## Links

- Root guide: `../../AGENTS.md`
- Dagster orchestration: `../dagster/AGENTS.md`
- Transformations: `../../libs/transformations/AGENTS.md`
- Schema mapper: `../../libs/spatial_utils/AGENTS.md`
