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

## Service readiness helpers
- `python scripts/wait_for_services.py`
- `python scripts/check_container_stability.py`
