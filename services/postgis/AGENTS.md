# services/postgis/ — Agent Guide

## What this directory is / owns

This directory owns PostGIS initialization and documents its role as **transient compute**.
All persistent data must live in MinIO (files) + MongoDB (ledger).

## Key invariants / non-negotiables

- **Never persist** durable datasets in PostGIS.
- **Ephemeral schemas per run**: create → use → drop (even on failure).

## Entry points / key files

- `init/`: PostGIS init scripts (extensions, baseline config)
- Dev configuration: `../../docker-compose.yaml`

## How to work here

- The pipeline creates schemas like `proc_<run_id_sanitized>` and drops them after export.
- Geometry column is standardized to `geom` in compute schemas.

## Common tasks

- **Add an extension / init SQL**: add it under `init/` and verify in integration tests.
- **Investigate leaked schemas** (dev): query `information_schema.schemata` for `proc_%` and drop them.

## Testing / verification

- Integration: `pytest -m integration tests/integration/test_postgis.py`
- Schema lifecycle: `pytest -m integration tests/integration/test_schema_cleanup.py`

## Links

- Root guide: `../../AGENTS.md`
- Dagster orchestration: `../dagster/AGENTS.md`
- Transformations: `../../libs/transformations/AGENTS.md`
