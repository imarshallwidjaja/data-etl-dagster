# tests/unit/ — Agent Guide

## What this directory is / owns

Fast, isolated unit tests that run without external services.

## Key invariants / non-negotiables

- No Docker required.
- Mock external boundaries:
  - **GDAL**: mock `subprocess.run`
  - **MinIO/S3**: mock `boto3` / clients
  - **MongoDB**: use `mongomock`

## Entry points / key files

- `test_models.py`: Pydantic model validation
- `test_schema_mapper.py`: schema mapping utility
- `test_*_resource.py`: Dagster resource wrappers
- `ops/`, `sensors/`, `libs/`: focused test subpackages

## How to work here

- Prefer small, focused tests.
- Validate edge cases (empty geometry bounds, invalid manifests, etc.).

## Running

```bash
pytest tests/unit -v
```

## Links

- Parent tests guide: `../AGENTS.md`
- Root guide: `../../AGENTS.md`
