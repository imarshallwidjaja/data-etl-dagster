# tests/ — Agent Guide

## What this directory is / owns

This directory contains the project test suite.
It is split into **unit tests** (fast, mocked) and **integration tests** (live services via Docker).

## Key invariants / non-negotiables

- **Unit tests must not require Docker** (mock external services and subprocess calls).
- **Integration tests must be explicit**: use `@pytest.mark.integration` and assume Docker is running.
- **E2E tests must be explicit**: use `@pytest.mark.e2e` (these are still integration tests and require Docker).
- **Test isolation for MongoDB**: Use `clean_mongodb` fixture for tests that need clean database state. This clears documents but preserves schemas and indexes.
- **Test isolation for MinIO**: Use `clean_minio` fixture for tests that need clean object storage state.

## Entry points / key files

- `conftest.py`: shared fixtures
- `unit/`: isolated tests (models, services, ops, sensors, webapp)
- `integration/`: tests against running services

## Test organization

### Unit tests (`unit/`)

| Directory/File | Description |
|----------------|-------------|
| `test_models.py` | Manifest/Asset model validation |
| `test_migration_runner.py` | MongoDB migration runner |
| `test_*_resource.py` | Resource mocking (MinIO, MongoDB, PostGIS) |
| `libs/` | Libs module tests (registry, s3_utils, transformations) |
| `ops/` | Dagster ops tests (cleanup, export, join, load, transform) |
| `sensors/` | Sensor tests (asset_sensors, manifest_sensor) |
| `webapp/` | Webapp unit tests (auth, manifest_builder, activity_router, activity_service) |

### Integration tests (`integration/`)

| File | Description |
|------|-------------|
| `test_*_e2e.py` | Pipeline E2E tests (spatial, tabular, join, ingest, sensor) |
| `test_minio*.py` | MinIO connectivity and operations |
| `test_mongodb*.py` | MongoDB connectivity and migrations |
| `test_postgis*.py` | PostGIS connectivity and init |
| `test_dagster*.py` | Dagster job execution |
| `test_webapp*.py` | Webapp health and CRUD operations |

## How to work here

- Add unit tests for all business logic changes.
- Add integration tests only when behavior depends on real services.

## Common tasks

- **Run unit tests**:

```bash
pytest tests/unit -v
```

- **Run integration tests** (Docker stack must be up):

```bash
docker compose up -d
python scripts/wait_for_services.py
pytest -m "integration and not e2e" tests/integration
pytest -m "integration and e2e" tests/integration
```

- **Run full E2E suite** (after metadata restructuring):

```bash
docker compose -f docker-compose.yaml up -d --build dagster-webserver dagster-daemon user-code minio minio-init mongodb postgis dagster-postgres
python scripts/wait_for_services.py

# Run all integration tests
pytest tests/integration -v -m "integration"

# Run just E2E tests
pytest tests/integration -v -m "e2e"
```

- **Run webapp tests**:

```bash
# Unit tests (in tests/unit/webapp/)
pytest tests/unit/webapp -v

# Integration tests
pytest -m integration tests/integration/test_webapp*.py -v
```

## Links

- Root guide: `../AGENTS.md`
- Unit tests: `unit/AGENTS.md`
- Integration tests: `integration/AGENTS.md`

