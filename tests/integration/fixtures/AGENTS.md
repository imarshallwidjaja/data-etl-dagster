# tests/integration/fixtures/ — Agent Guide

## What this directory is / owns

This directory contains test fixtures for integration and E2E tests. Fixtures include sample data files and manifest templates used to test the Dagster ETL pipelines.

## Key invariants / non-negotiables

- **Template placeholders must be substituted at runtime** - never upload templates directly
- **FileType enum values** must match `libs/models/spatial.py`: `"raster"`, `"vector"`, or `"tabular"` (NOT `"spatial"`)
- **Intent/type coherence** must be maintained (e.g., `ingest_tabular` requires all files with `type: "tabular"`)

## Entry points / key files

### Manifest templates (`asset_plans/`)

| File | Intent | Description |
|------|--------|-------------|
| `e2e_spatial_manifest.json` | `ingest_vector` | Spatial vector ingestion |
| `e2e_tabular_manifest.json` | `ingest_tabular` | Tabular CSV ingestion |
| `e2e_join_manifest.json` | `join_datasets` | Join workflow (asset-only, no files) |

### Sample data (`asset_plans/`)

| File | Description |
|------|-------------|
| `e2e_sample_sa1_data.json` | GeoJSON - 42 SA1 boundaries from NSW (CRS: EPSG:4283) |
| `e2e_sample_table_data.csv` | CSV - 42 rows of heat vulnerability indices |

### Legacy root files

| File | Description |
|------|-------------|
| `e2e_sample_sa1_data.json` | Full GeoJSON dataset |
| `e2e_sample_sa1_data-manifest.json` | Legacy manifest format |

## Template placeholders

Manifest JSON files contain placeholders that MUST be substituted at runtime:

| Placeholder | Description | Example Substitution |
|-------------|-------------|---------------------|
| `${UUID}` | Unique test run identifier | `abc123def456` |
| `${BATCH_ID}` | Full batch identifier | `sensor_e2e_tabular_abc123` |
| `${SPATIAL_ASSET_ID}` | MongoDB `_id` of the spatial asset (join only) | `507f1f77bcf86cd799439011` |
| `${TABULAR_ASSET_ID}` | MongoDB `_id` of the tabular asset (join only) | `507f1f77bcf86cd799439012` |

> **Note**: `SPATIAL_ASSET_ID` and `TABULAR_ASSET_ID` are MongoDB ObjectId strings, not `dataset_id` values. The test creates parent assets first and uses their `_id` fields.

## How to work here

- **Loading manifests in tests**: Use the `_load_manifest_template()` helper function pattern
- **Adding new fixtures**: Place in `asset_plans/` with descriptive names
- **Join key alignment**: Ensure spatial and tabular fixtures share a common join key column

### Example loading pattern

```python
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "asset_plans"
TABULAR_MANIFEST_TEMPLATE = FIXTURES_DIR / "e2e_tabular_manifest.json"

manifest = _load_manifest_template(
    template_path=TABULAR_MANIFEST_TEMPLATE,
    uuid=test_uuid,
    batch_id=batch_id,
    data_key=csv_key,
)
```

## Common errors

| Error | Cause |
|-------|-------|
| `"Input should be 'raster', 'vector' or 'tabular'"` | Wrong `type` value in manifest (e.g., using `"spatial"`) |
| `"source must be CopySource type"` | MinIO archive bug (fixed in `minio_resource.py`) |
| `"Partition not found"` | Sensor didn't create dynamic partition before RunRequest |

## Testing / verification

Fixtures are used by E2E tests:
- `test_spatial_asset_e2e.py`
- `test_tabular_asset_e2e.py`
- `test_join_asset_e2e.py`
- `test_sensor_e2e.py`

## Links

- Parent tests guide: `../../AGENTS.md`
- Integration tests guide: `../AGENTS.md`
- Root guide: `../../../AGENTS.md`
