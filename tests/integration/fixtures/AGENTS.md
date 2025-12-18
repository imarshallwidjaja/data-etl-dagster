# Test Fixtures - AI Agent Instructions

## Purpose

This directory contains test fixtures for integration and E2E tests. Fixtures include sample data files and manifest templates used to test the Dagster ETL pipelines.

## Directory Structure

```
fixtures/
├── asset_plans/           # Manifest templates and sample data for asset tests
│   ├── e2e_spatial_manifest.json    # Spatial ingestion manifest template
│   ├── e2e_tabular_manifest.json    # Tabular ingestion manifest template
│   ├── e2e_join_manifest.json       # Join workflow manifest template
│   ├── e2e_sample_sa1_data.json     # Sample GeoJSON (42 SA1 features)
│   └── e2e_sample_table_data.csv    # Sample CSV (42 rows, heat vulnerability)
├── e2e_sample_sa1_data.json         # Full GeoJSON dataset (larger version)
└── e2e_sample_sa1_data-manifest.json  # Legacy manifest format
```

## Manifest Templates

### Template Placeholders

Manifest JSON files contain placeholders that MUST be substituted at runtime:

| Placeholder | Description | Example Substitution |
|-------------|-------------|---------------------|
| `${UUID}` | Unique test run identifier | `abc123def456` |
| `${BATCH_ID}` | Full batch identifier | `sensor_e2e_tabular_abc123` |
| `${SPATIAL_ASSET_ID}` | MongoDB ObjectId of spatial asset (join only) | `507f1f77bcf86cd799439011` |
| `${TABULAR_ASSET_ID}` | MongoDB ObjectId of tabular asset (join only) | `507f1f77bcf86cd799439012` |

### Loading Manifests in Tests

Use the `_load_manifest_template()` helper function:

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

## Manifest Schema Requirements

### FileType Enum Values

The `files[].type` field MUST be one of:
- `"raster"` - For raster/imagery data
- `"vector"` - For vector/feature data (GeoJSON, GPKG, SHP)
- `"tabular"` - For tabular data (CSV)

⚠️ `"spatial"` is NOT valid - use `"vector"` or `"raster"` instead.

### Intent Values

| Intent | Description | Required Fields |
|--------|-------------|-----------------|
| `ingest_vector` | Spatial vector ingestion | `files[0]` with `type: "vector"` |
| `ingest_raster` | Raster ingestion | `files[0]` with `type: "raster"` |
| `ingest_tabular` | Tabular CSV ingestion | `files[0]` with `type: "tabular"` |
| `join_datasets` | Join spatial + tabular assets | `metadata.join_config`, `files: []` empty |

## Data Files

### e2e_sample_sa1_data.json (GeoJSON)

- 42 SA1 boundary features from NSW
- MultiPolygon geometries
- CRS: EPSG:4283 (GDA94)
- Join key column: `sa1_code21`

### e2e_sample_table_data.csv (CSV)

- 42 rows of heat vulnerability indices
- Columns include: `ogc_fid`, `sa1_code21`, `hhvi_*` metrics
- Join key column: `sa1_code21`

## Common Errors

1. **"Input should be 'raster', 'vector' or 'tabular'"** - Wrong `type` value in manifest
2. **"source must be CopySource type"** - MinIO archive bug (fixed in minio_resource.py)
3. **"Partition not found"** - Sensor didn't create dynamic partition before RunRequest
