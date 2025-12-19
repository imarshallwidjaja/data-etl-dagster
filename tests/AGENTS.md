# tests/ — Agent Guide

## What this directory is / owns

This directory contains the project test suite.
It is split into **unit tests** (fast, mocked) and **integration tests** (live services via Docker).

## Key invariants / non-negotiables

- **Unit tests must not require Docker** (mock external services and subprocess calls).
- **Integration tests must be explicit**: use `@pytest.mark.integration` and assume Docker is running.
- **E2E tests must be explicit**: use `@pytest.mark.e2e` (these are still integration tests and require Docker).

## Entry points / key files

- `conftest.py`: shared fixtures
- `unit/`: isolated tests
- `integration/`: tests against running services

## Test organization

### Unit tests (`unit/`)

| File | Description |
|------|-------------|
| `test_manifest_model.py` | Manifest/FileEntry model validation |
| `test_asset_model.py` | Asset model validation |
| `test_lineage.py` | Lineage data structure tests |

### Integration tests (`integration/`)

| File | Description |
|------|-------------|
| `test_spatial_asset_e2e.py` | Spatial asset pipeline E2E |
| `test_tabular_asset_e2e.py` | Tabular asset pipeline E2E |
| `test_join_asset_e2e.py` | Join asset pipeline E2E |
| `test_ingest_job_e2e.py` | Ingest job E2E |
| `test_sensor_e2e.py` | Sensor-triggered E2E |
| `test_mongodb_migrations.py` | MongoDB migration tests |
| `test_webapp_health.py` | Webapp health endpoints |
| `test_webapp_landing.py` | Webapp landing zone CRUD |
| `test_webapp_assets.py` | Webapp asset listing |

### Webapp unit tests (`services/webapp/tests/unit/`)

| File | Description |
|------|-------------|
| `test_auth.py` | Auth provider tests |
| `test_manifest_builder.py` | Form → Manifest conversion |
| `test_rerun_versioning.py` | Batch ID versioning |

## How to work here

- Add unit tests for all business logic changes.
- Add integration tests only when behavior depends on real services (MinIO/Mongo/PostGIS/Dagster/GDAL in container).

## Common tasks

- **Run unit tests**:

```bash
pytest tests/unit
```

- **Run integration tests** (Docker stack must be up and healthy):

```bash
docker compose up -d
python scripts/wait_for_services.py
pytest -m "integration and not e2e" tests/integration
pytest -m "integration and e2e" tests/integration
```

- **Run webapp tests**:

```bash
# Unit tests
pytest services/webapp/tests/unit -v

# Integration tests
pytest -m integration tests/integration/test_webapp*.py -v
```

## Links

- Root guide: `../AGENTS.md`
- Unit tests: `unit/AGENTS.md`
- Integration tests: `integration/AGENTS.md`
- Webapp tests: `../services/webapp/tests/`
