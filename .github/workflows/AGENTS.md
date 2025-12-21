# .github/workflows/ — Agent Guide

## What this directory is / owns

This directory contains GitHub Actions CI/CD workflows using an **orchestrator pattern** with reusable workflows.

## Key invariants / non-negotiables

- **E2E depends on all others**: E2E tests only run if unit + all integration tests pass.
- **Path filtering required**: Each workflow must only trigger for relevant file changes.
- **Reusable workflows prefixed with `_`**: Files like `_unit-tests.yml` are called by `ci.yml`.
- **No direct triggers on reusable workflows**: Only `ci.yml` should trigger on `push`/`pull_request`.

## Workflow structure

| File | Purpose |
|------|---------|
| `ci.yml` | Orchestrator: detects changes, calls reusable workflows, gates E2E |
| `_unit-tests.yml` | Unit tests (no Docker) |
| `_integration-infra.yml` | MinIO, MongoDB, PostGIS integration tests |
| `_integration-etl.yml` | Dagster ETL pipeline tests |
| `_integration-webapp.yml` | Webapp integration tests |
| `_e2e-tests.yml` | Full end-to-end pipeline tests |

## Path filter categories (in ci.yml)

| Category | Paths | Triggers |
|----------|-------|----------|
| `unit` | `libs/**`, `services/**`, `tests/unit/**` | Unit tests |
| `infra` | `services/minio\|mongodb\|postgis/**`, `tests/integration/test_minio*` etc. | Infra tests |
| `etl` | `services/dagster/**`, `libs/transformations/**`, `tests/integration/test_dagster*` etc. | ETL tests |
| `webapp` | `services/webapp/**`, `tests/integration/test_webapp*` | Webapp tests |
| `force-all` | `docker-compose.yaml`, `requirements*.txt`, `.github/workflows/**` | All tests |

## Environment variables

| Variable | Used by | Description |
|----------|---------|-------------|
| `WAIT_FOR_SERVICES` | `wait_for_services.py` | Comma-separated services to check (default: all) |
| `CHECK_CONTAINERS` | `check_container_stability.py` | Comma-separated containers (default: all) |
| `SERVICE_WAIT_TIMEOUT` | Both scripts | Seconds to wait per service (default: 60) |

## How to work here

- **Add a new test category**: Add path filter in `ci.yml`, create new `_integration-*.yml`
- **Modify path filters**: Edit the `filters` block in the `detect-changes` job
- **Skip tests**: Add `[skip ci]` to commit message
- **Force all tests**: Use manual `workflow_dispatch` trigger

## Common tasks

- **View workflow syntax errors**: Check Actions tab → select workflow → view annotations
- **Re-run failed jobs**: Click "Re-run failed jobs" in Actions tab
- **Debug path filtering**: Check `detect-changes` job outputs in workflow logs

## Links

- Root guide: `../../AGENTS.md`
- Scripts: `../../scripts/AGENTS.md`
- Tests: `../../tests/AGENTS.md`
