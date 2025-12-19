# Test Fixtures

Test data and manifest templates for integration and E2E tests.

## Contents

### 📁 asset_plans/

Manifest templates and sample datasets for asset-based pipeline tests.

| File | Description |
|------|-------------|
| `e2e_spatial_manifest.json` | Manifest template for spatial (vector) ingestion |
| `e2e_tabular_manifest.json` | Manifest template for tabular (CSV) ingestion |
| `e2e_join_manifest.json` | Manifest template for join workflow |
| `e2e_sample_sa1_data.json` | Sample GeoJSON - 42 SA1 boundaries from NSW |
| `e2e_sample_table_data.csv` | Sample CSV - Heat vulnerability indices |

### 📄 Root Files

| File | Description |
|------|-------------|
| `e2e_sample_sa1_data.json` | Full SA1 GeoJSON dataset |
| `e2e_sample_sa1_data-manifest.json` | Legacy manifest format |

## Manifest Templates

Manifests use placeholder syntax for dynamic values:

```json
{
    "batch_id": "e2e_spatial_${UUID}",
    "files": [{
        "path": "s3://landing-zone/e2e/${BATCH_ID}/data.json",
        "type": "vector",
        "format": "GeoJSON"
    }],
    "metadata": {
        "tags": {
            "dataset_id": "sa1_spatial_${UUID}"
        }
    }
}
```

### Placeholders

| Placeholder | Purpose |
|-------------|---------|
| `${UUID}` | Unique identifier for test isolation |
| `${BATCH_ID}` | Full batch identifier |
| `${SPATIAL_DATASET_ID}` | Dataset ID of the spatial asset (join tests) |
| `${TABULAR_DATASET_ID}` | Dataset ID of the tabular asset (join tests) |

### Join Config Version Pinning

The `join_config` supports optional version fields:

```json
"join_config": {
  "spatial_dataset_id": "sa1_spatial_102",
  "spatial_version": null,
  "tabular_dataset_id": "sa1_tabular_101",
  "tabular_version": null,
  "left_key": "sa1_code21"
}
```

- `spatial_version` / `tabular_version`: If `null` (or omitted), uses the **latest** version
- To pin a specific version, set to an integer (e.g., `"spatial_version": 2`)



## Usage in Tests

Tests load manifests using `_load_manifest_template()`:

```python
manifest = _load_manifest_template(
    template_path=TABULAR_MANIFEST_TEMPLATE,
    uuid=uuid4().hex[:8],
    batch_id=batch_id,
    data_key=csv_key,
)
```

This prevents drift between fixture documentation and test code.

## Sample Data

### GeoJSON (e2e_sample_sa1_data.json)

- **Source**: ABS SA1 boundaries, NSW subset
- **Features**: 42 SA1 polygons
- **CRS**: EPSG:4283 (GDA94)
- **Join Key**: `sa1_code21`

### CSV (e2e_sample_table_data.csv)

- **Source**: Heat vulnerability index sample
- **Rows**: 42 (matching SA1 features)
- **Columns**: `ogc_fid`, `sa1_code21`, `hhvi_*` metrics
- **Join Key**: `sa1_code21`

## Valid File Types

When creating manifests, use these `type` values:

| Type | Use For |
|------|---------|
| `"vector"` | GeoJSON, GPKG, Shapefile |
| `"raster"` | GeoTIFF, COG |
| `"tabular"` | CSV files |

> ⚠️ **Note**: `"spatial"` is not valid - use `"vector"` or `"raster"` instead.
