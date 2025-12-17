# tests/integration/ — Agent Guide

## What this directory is / owns

Integration tests validate behavior against **live services** running in Docker:
MinIO, MongoDB, PostGIS, Dagster (and GDAL availability inside the user-code container).

This directory includes:
- **Connectivity/health** tests for each service
- **GraphQL-launched E2E** tests that validate offline-first loops:
  - `ingest_job` (spatial, legacy op-based): landing-zone → PostGIS (ephemeral) → data-lake + MongoDB ledger + schema cleanup
  - `tabular_asset_job` (tabular, asset-based): landing-zone → data-lake + MongoDB ledger (no PostGIS)
  - `join_asset_job` (join, asset-based): landing-zone tabular + data-lake spatial → PostGIS join → data-lake joined + MongoDB ledger + lineage

## Key invariants / non-negotiables

- Mark integration tests with `@pytest.mark.integration`.
- These tests will fail if Docker is not running / services are not healthy.
- E2E tests must be explicitly marked with `@pytest.mark.e2e` (see `pytest.ini` markers).

## Entry points / key files

- `test_minio.py`, `test_mongodb.py`, `test_postgis.py`: service connectivity + basic operations
- `test_dagster.py`: Dagster GraphQL/API reachability
- `test_gdal_health.py`: GDAL tooling availability
- `test_schema_cleanup.py`: verifies ephemeral schema lifecycle
- `test_mongodb_init.py`: verifies MongoDB initialization (collections, indexes)
- `test_postgis_init.py`: verifies PostGIS initialization (extensions, utility functions)
- `test_ingest_job_e2e.py`: launches `ingest_job` via GraphQL and asserts lake + ledger + cleanup
- `test_ingest_tabular_e2e.py`: launches `tabular_asset_job` via GraphQL and asserts lake + ledger + header mapping + join key normalization
- `test_join_asset_e2e.py`: launches `join_asset_job` via GraphQL and asserts joined output + lineage record
- `fixtures/`: versioned E2E sample inputs used by `test_ingest_job_e2e.py`

## Running

```bash
docker compose up -d
python scripts/wait_for_services.py
pytest -m "integration and not e2e" tests/integration -v
pytest -m "integration and e2e" tests/integration -v
```

## Links

- Parent tests guide: `../AGENTS.md`
- Root guide: `../../AGENTS.md`
