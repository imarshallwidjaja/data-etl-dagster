# Testing Guide

## Unit tests
- Environment: `uv sync --frozen --group test` (creates `.venv` with Python 3.10).
- Command: `uv run pytest tests/unit`
- Legacy alternative (still works): `pip install -r requirements-test.txt && pytest tests/unit`

## Integration tests
- Require Docker services.
- Command: `uv run pytest -m "integration" tests/integration`

## E2E tests
- Require Docker services.
- Command: `uv run pytest -m "integration and e2e" tests/integration`

## Cleanup expectations
- Integration/E2E tests should clean artifacts they create (manifests, runs, assets).
- Set `PRESERVE_TEST_RUNS=1` to keep run records and activity logs for debugging.
- Cleanup must be performed within the test/fixture (use `try/finally`). Pre-clean fixtures do not do post-test cleanup.

## Dagster run tagging for tests
- GraphQL-launched runs: include `executionMetadata.tags` with `testing=true` (use `build_test_run_tags` in `tests/integration/helpers.py`).
- Manifest-driven sensors: set `metadata.tags.testing=true` so sensors propagate `testing=true` on RunRequests.

## Service readiness helpers
- `python scripts/wait_for_services.py`
- `python scripts/check_container_stability.py`

## Docker stack (local)
Start the stack:

```
docker compose -f docker-compose.yaml up -d --build \
    dagster-webserver dagster-daemon user-code minio minio-init mongodb postgis dagster-postgres
```

Stop the stack:

```
docker compose -f docker-compose.yaml down -v
```
