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
├── __init__.py          # Exports all public models
├── manifest.py          # Ingestion manifest models
├── asset.py             # Asset registry models
├── spatial.py           # Spatial-specific types (bounds, CRS, etc.)
├── config.py            # Configuration models
├── provenance.py        # Placeholder for future Run/Lineage models
└── py.typed             # PEP 561 marker for type checking support
```

## Input/Output Contracts

### Manifest Models (`libs/models/manifest.py`)

- **Manifest (Input):** User-uploaded JSON contract. Validates file entries, unique paths, and metadata.
- **ManifestRecord (Persisted):** Extends `Manifest` with runtime tracking (status, run ID, timestamps) for MongoDB storage.
- **Components:** `FileEntry`, `ManifestMetadata`, `ManifestStatus`, `S3Path`.

### Asset Model (`libs/models/asset.py`)

- **Asset:** Registry model for processed datasets. Tracks S3 location, version, spatial bounds, CRS, and metadata.
- **Components:** `AssetMetadata`, `S3Key`, `ContentHash` (validated SHA256).

### Spatial Types (`libs/models/spatial.py`)

Reusable types with validation logic:
- **CRS:** Validates EPSG codes, WKT, and PROJ strings.
- **Bounds:** Geographic bounding box (`minx`, `miny`, `maxx`, `maxy`).
- **Enums:** `FileType` (Raster/Vector), `OutputFormat` (GeoParquet, COG, GeoJSON).

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

## Configuration Requirements (`libs/models/config.py`)

Configuration models use `pydantic-settings` with explicit `validation_alias` to map environment variables to fields.

| Settings Class | Purpose | Key Env Vars |
|----------------|---------|--------------|
| `MinIOSettings` | S3-compatible storage | `MINIO_ENDPOINT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` |
| `MongoSettings` | Metadata ledger | `MONGO_HOST`, `MONGO_INITDB_ROOT_USERNAME`, `MONGO_DATABASE` |
| `PostGISSettings` | Transient spatial compute | `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_DB` |
| `DagsterPostgresSettings` | Dagster metadata DB | `DAGSTER_POSTGRES_HOST`, `DAGSTER_POSTGRES_USER` |

## Development Notes

- All models should include comprehensive docstrings.
- Use `Field(...)` for validation constraints and descriptions.
- Export all public models from `__init__.py`.
- Keep MongoDB schema validation in sync with Pydantic models.
- Use `model_dump(mode="json")` for JSON serialization.
- The `py.typed` marker file enables PEP 561 type checking support for downstream packages.

### Package Installation

The `libs/` directory is a proper Python package (`spatial-etl-libs`).
- **Local development:** Install with `pip install -e ./libs` (already included in `requirements-test.txt`)
- **Docker:** The `user-code` container installs `libs` during image build. Changes to `libs/` require rebuilding the container: `docker-compose build user-code`

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

**Test Implementation Notes:**

1. **Validator Testing:** Since Pydantic validators (using `BeforeValidator`) only run during model instantiation, tests for custom validators (CRS, ContentHash, S3Path, S3Key) call the validator functions directly or test via model instances.

2. **Test Coverage:** All validation logic is covered:
   - CRS validation (EPSG, WKT, PROJ formats)
   - Bounds validation (min/max constraints)
   - Manifest validation (required fields, unique paths, empty arrays)
   - ManifestRecord creation and status enum validation
   - Asset validation (content hash, version constraints, optional fields)
   - ContentHash validation (prefix, length, character validation)
   - S3Path/S3Key validation (format, normalization)
   - Config settings (environment variable loading, defaults, connection strings)

3. **Running Tests:**
   ```bash
   # Install test dependencies (includes base library deps)
   pip install -r requirements-test.txt
   
   # Run all tests
   pytest tests/ -v
   
   # Run with coverage
   pytest tests/ --cov=libs/models --cov-report=term-missing
   
   # Type checking
   mypy libs/models/ --ignore-missing-imports
   ```

4. **Dependencies:** 
   - Base library dependencies are in `requirements.txt` (pydantic, pydantic-settings)
   - Test dependencies are in `requirements-test.txt` (includes base requirements + pytest)
   - This structure ensures dependencies are shared between runtime and testing

### Provenance Models (Deferred)

The `provenance.py` module is a placeholder for future `Run` and `Lineage` models. Implementation is deferred until the Dagster run/lineage schema integration approach is determined. See MongoDB collections `runs` and `lineage` in `services/mongodb/init/01-init-db.js` for the target schema.
