# Library Context: Spatial Utils

## Overview

This library provides Python wrappers for spatial operations, primarily around GDAL command-line tools. It enables the ETL pipeline to perform raster/vector transformations in a testable, mockable manner.

## Responsibilities

1. **GDAL Wrapper:** Python interface to `ogr2ogr`, `gdal_translate`, `gdalwarp`, etc.
2. **Format Conversion:** Convert between spatial formats (SHP → GeoParquet, TIFF → COG).
3. **CRS Transformation:** Reproject spatial data between coordinate systems.
4. **Validation:** Verify spatial file integrity and metadata.

## Module Structure

```
libs/spatial_utils/
├── CONTEXT.md           # This file
├── __init__.py
├── gdal_wrapper.py      # GDAL CLI wrapper
├── converters.py        # Format conversion utilities
├── validators.py        # Spatial file validation
└── crs.py               # CRS handling utilities
```

## Input/Output Contracts

### GDALWrapper Interface

```python
class GDALWrapper:
    """Mockable wrapper for GDAL CLI operations."""
    
    def ogr2ogr(
        self,
        input_path: str,
        output_path: str,
        output_format: str = "Parquet",
        target_crs: str | None = None,
        **options
    ) -> subprocess.CompletedProcess:
        """Convert vector data using ogr2ogr."""
        ...
    
    def gdal_translate(
        self,
        input_path: str,
        output_path: str,
        output_format: str = "COG",
        **options
    ) -> subprocess.CompletedProcess:
        """Convert raster data using gdal_translate."""
        ...
```

### Testing Pattern

```python
# Production
resource = GDALWrapper()

# Testing (mocked)
mock_wrapper = Mock(spec=GDALWrapper)
mock_wrapper.ogr2ogr.return_value = CompletedProcess(args=[], returncode=0)
```

## Relation to Global Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                   Spatial Utils (This Library)                │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│   Dagster User Code                                           │
│         │                                                     │
│         ▼                                                     │
│   ┌──────────────────────┐                                   │
│   │ GDALResource         │───► subprocess ───► gdal CLI      │
│   │ (configurable)       │                                   │
│   └──────────────────────┘                                   │
│         │                                                     │
│         ▼                                                     │
│   File System / S3 (via /vsis3/)                             │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

## Configuration Requirements

### GDAL Environment Variables

| Variable | Description |
|----------|-------------|
| `GDAL_DATA` | Path to GDAL data files |
| `PROJ_LIB` | Path to PROJ data files |
| `CPL_VSIL_CURL_ALLOWED_EXTENSIONS` | Allowed extensions for /vsicurl/ |
| `AWS_ACCESS_KEY_ID` | For /vsis3/ access |
| `AWS_SECRET_ACCESS_KEY` | For /vsis3/ access |
| `AWS_S3_ENDPOINT` | MinIO endpoint for /vsis3/ (host:port, no scheme) |\n| `AWS_HTTPS` | Whether to use HTTPS for S3 requests (YES/NO) |\n| `AWS_VIRTUAL_HOSTING` | Use virtual-hosted-style requests (TRUE/FALSE); MinIO typically needs FALSE |

### S3 Virtual File System

GDAL can read/write directly to S3 using `/vsis3/`:

```python
# Read from MinIO
input_path = "/vsis3/landing-zone/batch_XYZ/file.tif"

# Write to MinIO
output_path = "/vsis3/data-lake/dataset_001/v1/data.parquet"
```

## Development Notes

- GDAL is installed in the user-code container (see Dockerfile.user-code)
- Use the `GDALResource` from Dagster for all GDAL operations (it's a ConfigurableResource)
- The resource wraps subprocess calls and is mockable for unit testing
- Error handling is done via `GDALResult.success` flag and `.stderr` output
- Test files should use small spatial datasets to keep tests fast
- For integration tests, verify GDAL installation using `tests/integration/test_gdal_health.py`

