# Library Context: Data Models

## Overview

This library contains all Pydantic models and schemas used throughout the Spatial ETL Pipeline. It ensures type safety and validation at the application level.

## Responsibilities

1. **Schema Definition:** Define data structures for manifests, assets, and metadata.
2. **Validation:** Runtime validation of all data passing through the system.
3. **Serialization:** JSON serialization/deserialization for API and storage.
4. **Type Safety:** Provide strict typing for IDE support and error prevention.

## Module Structure

```
libs/models/
├── CONTEXT.md           # This file
├── __init__.py
├── manifest.py          # Ingestion manifest models
├── asset.py             # Asset registry models
├── spatial.py           # Spatial-specific types (bounds, CRS, etc.)
└── config.py            # Configuration models
```

## Input/Output Contracts

### Manifest Model

```python
from pydantic import BaseModel, Field
from typing import Literal
from datetime import datetime

class FileEntry(BaseModel):
    path: str
    type: Literal["raster", "vector"]
    format: str
    crs: str

class ManifestMetadata(BaseModel):
    project: str
    description: str | None = None

class Manifest(BaseModel):
    batch_id: str
    uploader: str
    intent: str
    files: list[FileEntry]
    metadata: ManifestMetadata
    
    class Config:
        json_schema_extra = {
            "example": {
                "batch_id": "batch_001",
                "uploader": "system",
                "intent": "ingest_vector",
                "files": [{"path": "s3://...", "type": "vector", "format": "GPKG", "crs": "EPSG:4326"}],
                "metadata": {"project": "ALPHA"}
            }
        }
```

### Asset Model

```python
class Bounds(BaseModel):
    minx: float
    miny: float
    maxx: float
    maxy: float

class AssetMetadata(BaseModel):
    title: str
    description: str | None = None
    source: str | None = None
    license: str | None = None

class Asset(BaseModel):
    s3_key: str
    dataset_id: str
    version: int
    content_hash: str
    dagster_run_id: str
    format: Literal["geoparquet", "cog", "geojson"]
    crs: str
    bounds: Bounds
    metadata: AssetMetadata
    created_at: datetime
    updated_at: datetime
```

## Relation to Global Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Models (This Library)                      │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│   │   MinIO     │    │   Dagster   │    │   MongoDB   │     │
│   │  (JSON)     │───►│  (Python)   │───►│  (BSON)     │     │
│   └─────────────┘    └─────────────┘    └─────────────┘     │
│          │                  │                  │              │
│          └──────────────────┼──────────────────┘              │
│                             │                                 │
│                             ▼                                 │
│                    Pydantic Models                            │
│                    (Single Source of Truth)                   │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

## Configuration Requirements

### Pydantic Settings

```python
from pydantic_settings import BaseSettings

class MinIOSettings(BaseSettings):
    endpoint: str
    access_key: str
    secret_key: str
    use_ssl: bool = False
    landing_bucket: str = "landing-zone"
    lake_bucket: str = "data-lake"
    
    class Config:
        env_prefix = "MINIO_"
```

## Development Notes

- All models should include comprehensive docstrings
- Use `Field(...)` for validation constraints and descriptions
- Export all public models from `__init__.py`
- Keep MongoDB schema validation in sync with Pydantic models
- Use `model_dump(mode="json")` for JSON serialization

