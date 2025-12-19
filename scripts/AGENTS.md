# scripts/ — Agent Guide

## What this directory is / owns

This directory contains **utility scripts** for development, CI/CD, and operations.
These scripts support the Docker-based development workflow and integration testing.

## Key invariants / non-negotiables

- **Scripts must work in CI and locally**: assume Docker is available but not necessarily running.
- **Exit codes matter**: 0 = success, non-zero = failure (for CI/CD pipelines).
- **Dependency on libs**: scripts that import from `libs/` require `pip install -e ./libs` first.

## Entry points / key files

| Script | Purpose | Used By |
|--------|---------|---------|
| `wait_for_services.py` | Health check all services before tests | CI, local dev |
| `check_container_stability.py` | Monitor container restarts | CI |
| `migrate_db.py` | Run MongoDB schema migrations | Container startup, dev |

## How to work here

- Scripts should use `libs/models` for settings (env var loading).
- Prefer clear, structured output for CI/CD log readability.
- Use ASCII-only output for Windows console compatibility.

## Common tasks

- **Add a new utility script**: create `scripts/your_script.py`, document in this file.
- **Debug service startup issues**: run `python scripts/wait_for_services.py` to see which service fails.

## Testing / verification

Scripts are tested indirectly via CI pipeline. Direct tests exist for migration runner:
- `tests/unit/test_migration_runner.py`

## Links

- Root guide: `../AGENTS.md`
- MongoDB migrations: `../services/mongodb/AGENTS.md`
- CI workflow: `../.github/workflows/`
