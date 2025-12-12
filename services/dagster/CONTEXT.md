# Service Context: Dagster Orchestrator

## Overview

This directory contains the Dagster orchestration layer for the Spatial ETL Pipeline. Dagster manages workflow scheduling, sensor-based triggers, and execution of ETL jobs.

## Responsibilities

1. **Workflow Orchestration:** Define and execute ETL pipelines as Dagster jobs/assets.
2. **Sensor Management:** Monitor MinIO landing zone for new manifest files.
3. **Resource Management:** Provide configurable resources (MinIO, MongoDB, PostGIS, GDAL).
4. **Run History:** Track all pipeline executions, logs, and lineage.

## Architecture Components

### Containers

| Container | Purpose | Port |
|-----------|---------|------|
| `dagster-webserver` | UI for monitoring and manual triggers | 3000 |
| `dagster-daemon` | Background scheduler and sensor runner | - |
| `dagster-postgres` | Internal metadata database | 5433 |
| `user-code` | Python + GDAL execution environment | 4000 (gRPC) |

### Key Files

```
services/dagster/
├── CONTEXT.md           # This file
├── Dockerfile           # Webserver/Daemon image
├── Dockerfile.user-code # User code image with GDAL
├── dagster.yaml         # Dagster instance configuration
├── workspace.yaml       # Code location definitions
└── requirements.txt     # Python dependencies
```

## Input/Output Contracts

### Inputs (Sensors)

- **MinIO Manifest Sensor:** Listens for JSON files at `s3://landing-zone/manifests/`
- **Manifest Schema:** See root `CONTEXT.md` Section 4.3

### Outputs

- **Run Metadata:** Stored in `dagster-postgres`
- **Asset Materializations:** Logged with lineage information
- **Events:** Published for monitoring/alerting

## Resource Configuration

All resources are configured via environment variables using Pydantic settings models.

### Implemented Resources

#### MinIOResource (`etl_pipelines/resources/minio_resource.py`)

**Status:** ✅ Implemented

S3-compatible object storage operations for landing zone and data lake.

**Key Methods:**
- `list_manifests()`: List JSON files in `manifests/` prefix
- `get_manifest(key)`: Download and parse manifest JSON
- `move_to_archive(key)`: Move processed manifest to `archive/`
- `upload_to_lake(local_path, s3_key)`: Upload processed file to data lake
- `get_presigned_url(bucket, key)`: Generate temp URL for GDAL `/vsicurl/`

**Configuration:** Uses `MinIOSettings` from `libs.models.config`

**Testing:** Unit tests with mocked `minio.Minio` client in `tests/unit/test_minio_resource.py`

#### PostGISResource (`etl_pipelines/resources/postgis_resource.py`)

**Status:** ✅ Implemented

Manages ephemeral schema lifecycle for spatial compute operations. Implements the compute engine pattern: Load → Transform → Dump → Drop.

PostGIS is used as a **transient compute node**, NOT for data persistence. All permanent data lives in MinIO/MongoDB.

**Key Methods:**
- `ephemeral_schema(run_id)`: Context manager for schema creation/deletion
- `execute_sql(sql, schema)`: Run SQL within a specific schema
- `table_exists(schema, table)`: Check table existence
- `get_table_bounds(schema, table)`: Compute spatial extent using `ST_Extent()`
- `get_engine()`: Get SQLAlchemy engine (cached, with connection pooling)

**Schema Lifecycle:**
Each Dagster run creates an ephemeral schema with the pattern `proc_{run_id_sanitized}`:
```python
run_id = "abc12345-def6-7890-ijkl-mnop12345678"
# Becomes schema: proc_abc12345_def6_7890_ijkl_mnop12345678
```

The schema is **automatically dropped** when the context manager exits (even on exception).

**Configuration:** Uses `PostGISSettings` from `libs.models.config`

**Testing:** 
- Unit tests with mocked SQLAlchemy engine in `tests/unit/test_postgis_resource.py`
- Integration tests verify PostGIS connectivity in `tests/integration/test_postgis.py`

**Usage Example:**
```python
@op(required_resource_keys={"postgis"})
def process_spatial_data(context):
    postgis = context.resources.postgis
    
    with postgis.ephemeral_schema(context.run_id) as schema:
        # Schema: proc_run_id_xxxxx created and available
        
        # Load raw data
        postgis.execute_sql(
            "CREATE TABLE raw_input AS (SELECT * FROM ...)", 
            schema
        )
        
        # Transform
        postgis.execute_sql(
            "CREATE TABLE processed AS (SELECT ST_Transform(geom, 4326) FROM raw_input)",
            schema
        )
        
        # Export via ogr2ogr to S3
        bounds = postgis.get_table_bounds(schema, "processed")
        
    # Schema automatically dropped here
```

**Round-Trip Run ID Mapping:**
The `RunIdSchemaMapping` utility (in `libs/spatial_utils/schema_mapper.py`) enables bidirectional conversion:
```python
# Forward: run_id → schema_name
mapping = RunIdSchemaMapping.from_run_id(run_id)
schema_name = mapping.schema_name

# Reverse: schema_name → run_id (useful for monitoring/recovery)
reverse = RunIdSchemaMapping.from_schema_name(schema_name)
original_run_id = reverse.run_id
```

#### MongoDBResource (`etl_pipelines/resources/mongodb_resource.py`)

**Status:** ✅ Implemented

Serves as the metadata ledger interface that writes manifest status updates and asset registrations to MongoDB. This resource keeps the "Source of Truth" guarantee centralized so ops only interact with simple helpers like `insert_manifest`, `update_manifest_status`, `insert_asset`, `get_latest_asset`, and `get_next_version`.

**Key Methods:**
- `insert_manifest(record)`: Store manifests in the `manifests` collection.
- `update_manifest_status(batch_id, status, ...)`: Advance manifest lifecycle and capture timestamps.
- `get_manifest(batch_id)`: Load persisted manifest metadata.
- `insert_asset(asset)`: Register processed assets and content hashes.
- `get_latest_asset(dataset_id)` / `get_next_version(dataset_id)`: Track versioning for datasets.
- `asset_exists(content_hash)`: Deduplicate uploads before processing.

**Configuration:** Uses `MongoSettings` from `libs.models.config` (`MONGO_*` env vars).

**Testing:** `tests/unit/test_mongodb_resource.py` exercises every method via `mongomock`, keeping unit tests fast and isolated.

### Planned Resources

- **GDALResource:** Subprocess wrapper for GDAL CLI operations (mockable for unit tests)

## Relation to Global Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Dagster (This Service)                    │
├─────────────────────────────────────────────────────────────┤
│  Sensors ──► Daemon ──► User Code Container                 │
│                              │                               │
│                              ▼                               │
│              ┌───────────────┴───────────────┐              │
│              │                               │               │
│              ▼                               ▼               │
│         MinIO (S3)                    PostGIS (SQL)         │
│              │                               │               │
│              └───────────────┬───────────────┘              │
│                              │                               │
│                              ▼                               │
│                        MongoDB (Ledger)                      │
└─────────────────────────────────────────────────────────────┘
```

## Configuration Requirements

### Environment Variables (Required)

| Variable | Description |
|----------|-------------|
| `DAGSTER_POSTGRES_HOST` | Dagster DB hostname |
| `DAGSTER_POSTGRES_USER` | Dagster DB username |
| `DAGSTER_POSTGRES_PASSWORD` | Dagster DB password |
| `DAGSTER_POSTGRES_DB` | Dagster DB name |
| `MINIO_*` | MinIO connection settings |
| `MONGO_*` | MongoDB connection settings |
| `POSTGRES_*` | PostGIS connection settings |

## Development Notes

- The `user-code` container is separate to allow hot-reloading during development
- GDAL libraries are only installed in the `user-code` container
- Use `dagster dev` locally for faster iteration (requires local GDAL install)
- Code location lives at `services/dagster/etl_pipelines`
  - Dev: bind mount via docker-compose hot-reloads code changes
  - Prod: `Dockerfile.user-code` copies code into the image; rebuild required for changes
  - Add new assets/jobs/resources/sensors to `definitions.py` (`defs`) so Dagster loads them
- **libs hot-reload:** The `libs/` package is installed at build time, not hot-reloaded. After editing files in `libs/`, rebuild the container: `docker-compose build user-code`
- S3/MinIO access uses `dagster-aws` S3 resources pointed at the MinIO endpoint (S3-compatible); ensure credentials/endpoint match MinIO in env vars.

## Common Mistakes

- Version tracks differ: core Dagster (`dagster`, `dagster-webserver`) uses 1.x (e.g., 1.12.5); integrations (`dagster-postgres`, `dagster-aws`) use 0.28.x (e.g., 0.28.4). Pin explicitly to matching tracks to avoid resolver/runtime issues.
- Current pins (Dec 2025): `dagster==1.12.5`, `dagster-webserver==1.12.5`, `dagster-postgres==0.28.4`, `dagster-aws==0.28.4`. Update together to keep compatibility.

