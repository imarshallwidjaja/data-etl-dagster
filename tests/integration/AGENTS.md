# tests/integration/ — Agent Guide

## What this directory is / owns

Integration tests validate behavior against **live services** running in Docker:
MinIO, MongoDB, PostGIS, Dagster (and GDAL availability inside the user-code container).

This directory includes:
- **Connectivity/health** tests for each service
- **GraphQL-launched E2E** tests that validate offline-first loops:
  - `ingest_job` (spatial, legacy op-based): landing-zone → PostGIS (ephemeral) → data-lake + MongoDB ledger + schema cleanup
  - `spatial_asset_job` (spatial, asset-based): landing-zone → PostGIS (ephemeral) → data-lake + MongoDB ledger + schema cleanup
  - `tabular_asset_job` (tabular, asset-based): landing-zone → data-lake + MongoDB ledger (no PostGIS)
  - `join_asset_job` (join, asset-based): data-lake spatial + data-lake tabular → PostGIS join → data-lake joined + MongoDB ledger + lineage (both parents must be existing assets)

## Key invariants / non-negotiables

- Mark integration tests with `@pytest.mark.integration`.
- These tests will fail if Docker is not running / services are not healthy.
- E2E tests must be explicitly marked with `@pytest.mark.e2e` (see `pytest.ini` markers).

## Entry points / key files

### Service connectivity tests

- `test_minio.py`: MinIO connectivity + basic operations
- `test_mongodb.py`: MongoDB connectivity + basic operations
- `test_postgis.py`: PostGIS connectivity + basic operations
- `test_dagster.py`: Dagster GraphQL/API reachability
- `test_gdal_health.py`: GDAL tooling availability inside user-code container

### Initialization tests

- `test_mongodb_init.py`: verifies MongoDB initialization (collections, indexes)
- `test_mongodb_migrations.py`: verifies MongoDB migration runner and schema evolution
- `test_postgis_init.py`: verifies PostGIS initialization (extensions, utility functions)
- `test_schema_cleanup.py`: verifies ephemeral schema lifecycle

### E2E pipeline tests

- `test_ingest_job_e2e.py`: launches `ingest_job` (legacy op-based) via GraphQL
- `test_spatial_asset_e2e.py`: launches `spatial_asset_job` via GraphQL with partition key
- `test_tabular_asset_e2e.py`: launches `tabular_asset_job` via GraphQL with partition key
- `test_join_asset_e2e.py`: launches `join_asset_job` via GraphQL, asserts joined output + lineage
- `test_sensor_e2e.py`: tests sensor-triggered pipelines (manifest detection → job launch)
- `test_sensor_partition_creation.py`: tests dynamic partition creation by sensors

### Fixtures

- `fixtures/`: versioned sample data and manifest templates used by E2E tests (see `fixtures/AGENTS.md`)

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
