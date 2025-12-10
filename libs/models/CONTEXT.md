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
from pydantic import BaseModel, Field, model_validator
from datetime import datetime
from .spatial import CRS, FileType
from .manifest import S3Path  # Validated S3 path type

class FileEntry(BaseModel):
    path: S3Path  # Validated S3 path (normalized with s3:// prefix)
    type: FileType  # Enum: RASTER or VECTOR
    format: str  # Input format (e.g., "GTiff", "GPKG", "SHP")
    crs: CRS  # Validated CRS type (EPSG, WKT, or PROJ)

class ManifestMetadata(BaseModel):
    project: str
    description: str | None = None

class Manifest(BaseModel):
    """User-uploaded manifest contract. This is what gets uploaded to s3://landing-zone/manifests/"""
    batch_id: str
    uploader: str
    intent: str
    files: list[FileEntry]  # min_length=1, validated for unique paths
    metadata: ManifestMetadata
    
    @model_validator(mode='after')
    def validate_unique_paths(self) -> 'Manifest':
        """Ensures all file paths in the manifest are unique."""
        ...
```

**Key Features:**
- `S3Path` type validates and normalizes S3 paths (adds `s3://` prefix if missing)
- `FileEntry.path` uses validated `S3Path` type
- `Manifest.files` validates that all paths are unique (prevents duplicate processing)
- All file paths must be unique within a manifest

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
    
    @classmethod
    def from_manifest(
        cls,
        manifest: Manifest,
        status: ManifestStatus = ManifestStatus.PENDING,
        ingested_at: datetime | None = None
    ) -> 'ManifestRecord':
        """Convenience method to create a ManifestRecord from a Manifest."""
        ...
```

**Key Features:**
- Extends `Manifest` (inherits all fields)
- Adds runtime tracking: `status`, `dagster_run_id`, `error_message`, timestamps
- `from_manifest()` helper method simplifies conversion from input manifest to persisted record

### Asset Model

```python
from .spatial import Bounds, CRS, OutputFormat
from .asset import S3Key, ContentHash

class AssetMetadata(BaseModel):
    title: str
    description: str | None = None
    source: str | None = None
    license: str | None = None

class Asset(BaseModel):
    s3_key: S3Key  # Validated S3 object key (no leading/trailing slashes)
    dataset_id: str
    version: int  # Field(ge=1)
    content_hash: ContentHash  # Validated: sha256:<64 hex chars>
    dagster_run_id: str
    format: OutputFormat  # Enum: GEOPARQUET, COG, GEOJSON
    crs: CRS  # Validated CRS type
    bounds: Bounds  # From spatial.py
    metadata: AssetMetadata
    created_at: datetime
    updated_at: datetime | None = None
    
    def get_full_s3_path(self, bucket: str) -> str:
        """Get full S3 path: s3://{bucket}/{s3_key}"""
        ...
    
    def get_s3_key_pattern(self) -> str:
        """Get expected S3 key pattern: {dataset_id}/v{version}/..."""
        ...
```

**Key Features:**
- `S3Key` type validates object key format (non-empty, no leading/trailing slashes)
- `ContentHash` type validates SHA256 hash format (`sha256:<64 hex chars>`), normalizes to lowercase
- `version` must be >= 1
- Helper methods: `get_full_s3_path()` and `get_s3_key_pattern()`

### Spatial Types

The `spatial.py` module provides reusable spatial data types:

#### CRS (Coordinate Reference System)

The `CRS` type is a validated string that supports multiple CRS formats:

- **EPSG codes**: `EPSG:4326`, `EPSG:28354` (case-insensitive, normalized to uppercase)
- **WKT strings**: `PROJCS[...]`, `GEOGCS[...]`, `COMPD_CS[...]`, `GEOCCS[...]`
- **PROJ strings**: `+proj=utm +zone=55 +south ...`

**Validation:**
- Uses `BeforeValidator` to validate before type coercion
- Raises `TypeError` if input is not a string
- Raises `ValueError` with descriptive message if format is invalid
- EPSG codes are automatically normalized to uppercase (e.g., `epsg:4326` → `EPSG:4326`)

#### Bounds

```python
class Bounds(BaseModel):
    minx: float  # Minimum X coordinate (west)
    miny: float  # Minimum Y coordinate (south)
    maxx: float  # Maximum X coordinate (east)
    maxy: float  # Maximum Y coordinate (north)
    
    @model_validator(mode='after')
    def validate_bounds(self) -> 'Bounds':
        # Ensures minx < maxx and miny < maxy
        # Raises ValueError if validation fails
        ...
    
    @property
    def width(self) -> float:
        """Calculate the width (east-west extent) of the bounding box."""
        return self.maxx - self.minx
    
    @property
    def height(self) -> float:
        """Calculate the height (north-south extent) of the bounding box."""
        return self.maxy - self.miny
    
    @property
    def area(self) -> float:
        """Calculate the area of the bounding box (width × height)."""
        return self.width * self.height
```

**Key Features:**
- Validates that `minx < maxx` and `miny < maxy` (strict, no zero-area bounds)
- Provides helper properties: `width`, `height`, `area`
- All coordinates are `float` values

#### FileType and OutputFormat Enums

```python
class FileType(str, Enum):
    RASTER = "raster"
    VECTOR = "vector"

class OutputFormat(str, Enum):
    GEOPARQUET = "geoparquet"
    COG = "cog"
    GEOJSON = "geojson"
```

### S3 Path Types

#### S3Path (Full Path)

The `manifest.py` module provides `S3Path` type for validated full S3 object paths:

- Validates bucket name format (3-63 chars, lowercase alphanumeric)
- Normalizes paths to include `s3://` prefix
- Validates that both bucket name and object key are present
- Used by `FileEntry.path` for input file references

#### S3Key (Object Key Only)

The `asset.py` module provides `S3Key` type for validated S3 object keys (without bucket):

- Validates that key is non-empty
- Ensures no leading or trailing slashes
- Used by `Asset.s3_key` for data lake object keys
- Example: `"data-lake/dataset_001/v1/data.parquet"`

### Content Hash Type

The `asset.py` module provides `ContentHash` type for validated SHA256 hashes:

- Validates format: `sha256:<64 hexadecimal characters>`
- Automatically normalizes to lowercase
- Raises `TypeError` for non-string inputs
- Raises `ValueError` for invalid format
- Used by `Asset.content_hash`

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

All configuration models use `pydantic-settings` with `BaseSettings` and explicit environment variable mappings using `validation_alias`.

#### MinIO Settings

```python
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class MinIOSettings(BaseSettings):
    endpoint: str = Field(..., validation_alias="MINIO_ENDPOINT")
    access_key: str = Field(..., validation_alias="MINIO_ROOT_USER")
    secret_key: str = Field(..., validation_alias="MINIO_ROOT_PASSWORD")
    use_ssl: bool = Field(False, validation_alias="MINIO_USE_SSL")
    landing_bucket: str = Field("landing-zone", validation_alias="MINIO_LANDING_BUCKET")
    lake_bucket: str = Field("data-lake", validation_alias="MINIO_LAKE_BUCKET")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
    )
```

#### MongoDB Settings

```python
class MongoSettings(BaseSettings):
    host: str = Field("mongodb", validation_alias="MONGO_HOST")
    port: int = Field(27017, validation_alias="MONGO_PORT")
    username: str = Field(..., validation_alias="MONGO_INITDB_ROOT_USERNAME")
    password: str = Field(..., validation_alias="MONGO_INITDB_ROOT_PASSWORD")
    database: str = Field("spatial_etl", validation_alias="MONGO_DATABASE")
    auth_source: str = Field("admin", validation_alias="MONGO_AUTH_SOURCE")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
    )
    
    @property
    def connection_string(self) -> str:
        """Build MongoDB connection URI."""
        return f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?authSource={self.auth_source}"
```

#### PostGIS Settings (Spatial Compute Engine)

```python
class PostGISSettings(BaseSettings):
    """Settings for the transient PostGIS compute engine (port 5432)."""
    host: str = Field("postgis", validation_alias="POSTGRES_HOST")
    port: int = Field(5432, validation_alias="POSTGRES_PORT")
    user: str = Field(..., validation_alias="POSTGRES_USER")
    password: str = Field(..., validation_alias="POSTGRES_PASSWORD")
    database: str = Field("spatial_compute", validation_alias="POSTGRES_DB")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
    )
    
    @property
    def connection_string(self) -> str:
        """Build PostgreSQL connection URI."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
```

#### Dagster Postgres Settings (Internal Metadata DB)

```python
class DagsterPostgresSettings(BaseSettings):
    """Settings for Dagster's internal metadata database (port 5433)."""
    host: str = Field(..., validation_alias="DAGSTER_POSTGRES_HOST")
    port: int = Field(5433, validation_alias="DAGSTER_POSTGRES_PORT")
    user: str = Field(..., validation_alias="DAGSTER_POSTGRES_USER")
    password: str = Field(..., validation_alias="DAGSTER_POSTGRES_PASSWORD")
    database: str = Field(..., validation_alias="DAGSTER_POSTGRES_DB")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
    )
    
    @property
    def connection_string(self) -> str:
        """Build PostgreSQL connection URI."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
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

