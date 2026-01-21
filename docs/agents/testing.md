# Testing Guide

## Unit tests
- Run with pytest in the conda env.
- Conda env name: `data-etl-dagster`.
- Command: `pytest tests/unit`

## Integration tests
- Require Docker services.
- Command: `pytest -m "integration" tests/integration`

## E2E tests
- Require Docker services.
- Command: `pytest -m "integration and e2e" tests/integration`

## Cleanup expectations
- Integration/E2E tests should clean artifacts they create (manifests, runs, assets).
- Set `PRESERVE_TEST_RUNS=1` to keep run records and activity logs for debugging.

## Service readiness helpers
- `python scripts/wait_for_services.py`
- `python scripts/check_container_stability.py`
