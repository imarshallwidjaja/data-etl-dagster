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
├── config.py            # Configuration models
└── provenance.py        # Placeholder for future Run/Lineage models
```

## Input/Output Contracts

### Manifest Models

The system distinguishes between **input manifests** (what users upload) and **persisted manifests** (what gets stored in MongoDB with runtime tracking).

#### Manifest (Input Contract)

```python
from pydantic import BaseModel, Field
from typing import Literal
from datetime import datetime
from .spatial import CRS, FileType

class FileEntry(BaseModel):
    path: str
    type: FileType  # Literal["raster", "vector"]
    format: str
    crs: CRS  # Validated CRS type (EPSG, WKT, or PROJ)

class ManifestMetadata(BaseModel):
    project: str
    description: str | None = None

class Manifest(BaseModel):
    """User-uploaded manifest contract. This is what gets uploaded to s3://landing-zone/manifests/"""
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
                "files": [{"path": "s3://landing-zone/batch_001/file.gpkg", "type": "vector", "format": "GPKG", "crs": "EPSG:4326"}],
                "metadata": {"project": "ALPHA"}
            }
        }
```

#### ManifestRecord (Persisted Contract)

```python
class ManifestStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class ManifestRecord(Manifest):
    """Persisted manifest in MongoDB. Extends Manifest with runtime tracking fields."""
    status: ManifestStatus
    dagster_run_id: str | None = None
    error_message: str | None = None
    ingested_at: datetime
    completed_at: datetime | None = None
```

### Asset Model

```python
from .spatial import Bounds, CRS, OutputFormat

class AssetMetadata(BaseModel):
    title: str
    description: str | None = None
    source: str | None = None
    license: str | None = None

class Asset(BaseModel):
    s3_key: str
    dataset_id: str
    version: int  # Field(ge=1)
    content_hash: str  # Validated: ^sha256:[a-f0-9]{64}$
    dagster_run_id: str
    format: OutputFormat  # Literal["geoparquet", "cog", "geojson"]
    crs: CRS  # Validated CRS type
    bounds: Bounds  # From spatial.py
    metadata: AssetMetadata
    created_at: datetime
    updated_at: datetime | None = None
```

### Spatial Types

The `spatial.py` module provides reusable spatial data types:

#### CRS (Coordinate Reference System)

The `CRS` type is a validated string that supports multiple CRS formats:

- **EPSG codes**: `EPSG:4326`, `EPSG:28354` (case-insensitive, normalized to uppercase)
- **WKT strings**: `PROJCS[...]`, `GEOGCS[...]`, `COMPD_CS[...]`, `GEOCCS[...]`
- **PROJ strings**: `+proj=utm +zone=55 +south ...`

Invalid formats raise `ValueError` with a descriptive message.

#### Bounds

```python
class Bounds(BaseModel):
    minx: float
    miny: float
    maxx: float
    maxy: float
    
    @field_validator('*')
    def validate_bounds(cls, v, info):
        # Ensures minx < maxx and miny < maxy
        ...
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

All configuration models use `pydantic-settings` with `BaseSettings` and environment variable prefixes.

#### MinIO Settings

```python
from pydantic_settings import BaseSettings

class MinIOSettings(BaseSettings):
    endpoint: str
    access_key: str  # Maps from MINIO_ROOT_USER
    secret_key: str  # Maps from MINIO_ROOT_PASSWORD
    use_ssl: bool = False
    landing_bucket: str = "landing-zone"
    lake_bucket: str = "data-lake"
    
    class Config:
        env_prefix = "MINIO_"
```

#### MongoDB Settings

```python
class MongoSettings(BaseSettings):
    host: str = "mongodb"
    port: int = 27017
    username: str  # Maps from MONGO_INITDB_ROOT_USERNAME
    password: str  # Maps from MONGO_INITDB_ROOT_PASSWORD
    database: str = "spatial_etl"  # Maps from MONGO_INITDB_DATABASE
    auth_source: str = "admin"
    
    class Config:
        env_prefix = "MONGO_"
    
    @property
    def connection_string(self) -> str:
        """Build MongoDB connection URI."""
        ...
```

#### PostGIS Settings (Spatial Compute Engine)

```python
class PostGISSettings(BaseSettings):
    """Settings for the transient PostGIS compute engine (port 5432)."""
    host: str = "postgis"
    port: int = 5432
    user: str
    password: str
    database: str = "spatial_compute"
    
    class Config:
        env_prefix = "POSTGRES_"
    
    @property
    def connection_string(self) -> str:
        """Build PostgreSQL connection URI."""
        ...
```

#### Dagster Postgres Settings (Internal Metadata DB)

```python
class DagsterPostgresSettings(BaseSettings):
    """Settings for Dagster's internal metadata database (port 5433)."""
    host: str
    port: int = 5433
    user: str
    password: str
    database: str
    
    class Config:
        env_prefix = "DAGSTER_POSTGRES_"
```

**Note:** These are two separate PostgreSQL databases:
- **PostGIS** (`POSTGRES_*`): Transient spatial compute engine (ephemeral schemas)
- **Dagster Postgres** (`DAGSTER_POSTGRES_*`): Dagster's internal workflow metadata store

## Development Notes

- All models should include comprehensive docstrings
- Use `Field(...)` for validation constraints and descriptions
- Export all public models from `__init__.py`
- Keep MongoDB schema validation in sync with Pydantic models
- Use `model_dump(mode="json")` for JSON serialization

### Testing Strategy

Tests are located in `tests/unit/test_models.py` with shared fixtures defined in `tests/conftest.py`.

**Fixture Pattern:**
- `conftest.py` provides reusable test data fixtures (e.g., `valid_manifest_dict`, `valid_asset_dict`)
- Tests use fixtures to avoid duplicating sample data
- Fixtures can be modified in-place for negative test cases

**Example:**
```python
# tests/conftest.py
@pytest.fixture
def valid_manifest_dict():
    return {"batch_id": "...", ...}

# tests/unit/test_models.py
def test_valid_manifest(valid_manifest_dict):
    model = Manifest(**valid_manifest_dict)
    assert model.batch_id == "..."
```

### Provenance Models (Deferred)

The `provenance.py` module is a placeholder for future Run and Lineage models. These will be implemented once the Dagster run/lineage schema integration approach is determined. See MongoDB collections `runs` and `lineage` in `services/mongodb/init/01-init-db.js` for the target schema.

