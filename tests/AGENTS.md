# tests/ — Agent Guide

## What this directory is / owns

This directory contains the project test suite.
It is split into **unit tests** (fast, mocked) and **integration tests** (live services via Docker).

## Key invariants / non-negotiables

- **Unit tests must not require Docker** (mock external services and subprocess calls).
- **Integration tests must be explicit**: use `@pytest.mark.integration` and assume Docker is running.

## Entry points / key files

- `conftest.py`: shared fixtures
- `unit/`: isolated tests
- `integration/`: tests against running services

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
pytest -m integration tests/integration
```

## Links

- Root guide: `../AGENTS.md`
- Unit tests: `unit/AGENTS.md`
- Integration tests: `integration/AGENTS.md`
