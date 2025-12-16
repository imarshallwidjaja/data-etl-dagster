# libs/spatial_utils/ — Agent Guide

## What this directory is / owns

This library contains **lightweight spatial-adjacent utilities** used by the pipeline.
It owns the run-id ↔ PostGIS schema mapping logic and tabular header normalization utilities.

## Key invariants / non-negotiables

- **Schema names must be safe SQL identifiers**.
- **Mapping must be reversible** (schema → run_id) for observability and cleanup.
- **Do not add heavy GDAL bindings here** (GDAL stays in the `user-code` container).

## Entry points / key files

- `schema_mapper.py`: `RunIdSchemaMapping` (safe, reversible schema naming)
- `tabular_headers.py`: `normalize_headers` (Postgres identifier normalization for tabular data)
- `__init__.py`: exports

## How to work here

- Keep utilities small and dependency-light.
- If you need GDAL configuration/env var documentation, put it under `libs/models/AGENTS.md` (settings) or `services/dagster/AGENTS.md` (runtime/container).

## Common tasks

- **Change schema naming rules**: update `RunIdSchemaMapping`, then update unit tests.
- **Modify header cleaning rules**: update `normalize_headers` in `tabular_headers.py`, then update unit tests.

## Testing / verification

- Unit tests: `tests/unit/test_schema_mapper.py`

## Links

- Root guide: `../../AGENTS.md`
- Settings/models: `../models/AGENTS.md`
