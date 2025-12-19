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

### Root-level tests

- `test_models.py`: Pydantic model validation (manifest, asset, spatial types)
- `test_schema_mapper.py`: schema mapping utility
- `test_minio_resource.py`: MinIO resource wrapper
- `test_mongodb_resource.py`: MongoDB resource wrapper
- `test_postgis_resource.py`: PostGIS resource wrapper
- `test_migration_runner.py`: MongoDB migration runner

### Subpackages

- `libs/`: shared library tests
  - `test_tabular_headers.py`: header normalization
  - `test_registry.py`: recipe registry
  - `test_vector_steps.py`: vector transformation steps
  - `test_transformations_base.py`: base transformation interfaces
  - `test_s3_utils.py`: S3 utilities

- `ops/`: Dagster op tests
  - `test_load_op.py`: load operations
  - `test_transform_op.py`: transformation operations
  - `test_export_op.py`: export operations
  - `test_cleanup_op.py`: schema cleanup operations
  - `test_tabular_ops.py`: tabular processing operations
  - `test_join_ops.py`: join operations

- `sensors/`: Dagster sensor tests
  - `test_manifest_sensor.py`: manifest sensor (legacy)
  - `test_asset_sensors.py`: asset-based sensors

- `resources/`: Dagster resource tests
  - `test_gdal_resource.py`: GDAL resource wrapper

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
