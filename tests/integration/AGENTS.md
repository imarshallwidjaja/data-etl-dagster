# tests/integration/ — Agent Guide

## What this directory is / owns

Integration tests validate behavior against **live services** running in Docker:
MinIO, MongoDB, PostGIS, Dagster (and GDAL availability inside the user-code container).

**Note:** Current integration tests are connectivity/health tests. An `ingest_job` E2E test is planned for Phase 6.

## Key invariants / non-negotiables

- Mark integration tests with `@pytest.mark.integration`.
- These tests will fail if Docker is not running / services are not healthy.

## Entry points / key files

- `test_minio.py`, `test_mongodb.py`, `test_postgis.py`: service connectivity + basic operations
- `test_dagster.py`: Dagster GraphQL/API reachability
- `test_gdal_health.py`: GDAL tooling availability
- `test_schema_cleanup.py`: verifies ephemeral schema lifecycle

## Running

```bash
docker compose up -d
python scripts/wait_for_services.py
pytest -m integration tests/integration -v
```

## Links

- Parent tests guide: `../AGENTS.md`
- Root guide: `../../AGENTS.md`
