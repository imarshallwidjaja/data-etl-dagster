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

All resources are registered in `definitions.py` and configured using Dagster's `EnvVar` for environment variable resolution. This allows environment variables to be resolved at runtime and provides better visibility in the Dagster UI.

Bucket names and database names are hardcoded in `definitions.py` to match architectural defaults:
- MinIO buckets: `landing-zone`, `data-lake`
- MongoDB database: `spatial_etl`
- PostGIS database: `spatial_compute`

Note: The Pydantic Settings models (`MinIOSettings`, `MongoSettings`, `PostGISSettings`, `GDALSettings`) in `libs.models.config` are still available for use in standalone scripts and other contexts, but are no longer used in `definitions.py`.

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

**Configuration:** Uses `EnvVar` for `MINIO_ENDPOINT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`. Bucket names are hardcoded: `landing-zone`, `data-lake`.

**Testing:** Unit tests with mocked `minio.Minio` client in `tests/unit/test_minio_resource.py`

#### PostGISResource (`etl_pipelines/resources/postgis_resource.py`)

**Status:** ✅ Implemented

Manages ephemeral schema lifecycle for spatial compute operations. Implements the compute engine pattern: Load → Transform → Dump → Drop.

PostGIS is used as a **transient compute node**, NOT for data persistence. All permanent data lives in MinIO/MongoDB.

**Key Methods:**
- `ephemeral_schema(run_id)`: Context manager for schema creation/deletion
- `execute_sql(sql, schema)`: Run SQL within a specific schema
- `table_exists(schema, table)`: Check table existence
- `get_table_bounds(schema, table, geom_column="geom")`: Compute spatial extent using `ST_Extent()` on specified geometry column, returns None for empty geometry
- `get_engine()`: Get SQLAlchemy engine (cached, with connection pooling)

**Schema Lifecycle:**
Each Dagster run creates an ephemeral schema with the pattern `proc_{run_id_sanitized}`:
```python
run_id = "abc12345-def6-7890-ijkl-mnop12345678"
# Becomes schema: proc_abc12345_def6_7890_ijkl_mnop12345678
```

The schema can be managed in two ways:
1. **Context Manager** (recommended for standalone scripts): The `ephemeral_schema()` context manager automatically drops the schema when it exits (even on exception).
2. **Manual Management** (used by ETL ops): The ETL pipeline ops (`load_to_postgis`, `spatial_transform`, `export_to_datalake`) manually create schemas in `load_to_postgis` and automatically clean them up in `export_to_datalake` using a try/finally block. This ensures cleanup happens even if the export fails, maintaining the architectural law that PostGIS is transient compute only.

**Configuration:** Uses `EnvVar` for `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`. Database name is hardcoded: `spatial_compute`. Port defaults to `5432`.

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

**Configuration:** Uses `EnvVar("MONGO_CONNECTION_STRING")` for the connection URI. Database name is hardcoded: `spatial_etl`. The connection string can be built from individual env vars (see `MongoSettings.connection_string` property in `libs.models.config`) or set directly as `MONGO_CONNECTION_STRING`.

**Testing:** `tests/unit/test_mongodb_resource.py` exercises every method via `mongomock`, keeping unit tests fast and isolated.

#### GDALResource (`etl_pipelines/resources/gdal_resource.py`)

**Status:** ✅ Implemented (Phase 2.4)

Thin wrapper for GDAL CLI operations (ogr2ogr, gdal_translate, ogrinfo, gdalinfo).

**Key Features:**
- Stateless wrapper around subprocess calls
- Serializable I/O (dataclass result, string inputs)
- S3 support via `/vsis3/` virtual file system for MinIO access
- Designed for future Dagster Pipes migration

**Key Methods:**
- `ogr2ogr(input_path, output_path, ...)`: Convert vector data (GeoJSON → PostgreSQL, Shapefile → Parquet, etc.)
- `gdal_translate(input_path, output_path, ...)`: Convert raster data (GeoTIFF → COG, etc.)
- `ogrinfo(input_path, layer=...)`: Inspect vector datasets
- `gdalinfo(input_path)`: Inspect raster datasets

**Design Principles:**
- **Stateless:** All inputs are primitives (strings, dicts), no instance state
- **Serializable:** Returns `GDALResult` dataclass with JSON-serializable fields
- **Pipes-Ready:** Can be moved to separate process/container without modification
- **S3-Compatible:** Uses MinIO credentials via `/vsis3/` virtual file system

**Configuration:** Uses `EnvVar` for MinIO credentials: `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_ENDPOINT` (reused from MinIO config). GDAL/PROJ paths (`GDAL_DATA`, `PROJ_LIB`) are optional and typically set in Dockerfile.

**Testing:**
- Unit tests: `tests/unit/resources/test_gdal_resource.py` (mock `subprocess.run`)
- Integration tests: `tests/integration/test_gdal_health.py` (verify GDAL install/formats)

**Usage Example:**
```python
@op(required_resource_keys={"gdal"})
def load_vector_to_postgis(context):
    gdal = context.resources.gdal
    
    result = gdal.ogr2ogr(
        input_path="/vsis3/landing-zone/batch_123/data.geojson",
        output_path="PG:host=postgis dbname=spatial_compute",
        layer_name="raw_input",
        target_crs="EPSG:4326",
    )
    
    if not result.success:
        raise RuntimeError(f"ogr2ogr failed: {result.stderr}")
    
    context.log.info(f"✅ Loaded data to PostGIS")
    return result
```

### Implemented Ops

The ETL pipeline consists of four main ops that implement the Load → Transform → Dump → Drop pattern:

#### load_to_postgis (`etl_pipelines/ops/load_op.py`)

**Status:** ✅ Implemented

Loads spatial data from MinIO landing zone to PostGIS ephemeral schema using GDAL ogr2ogr.

**Key Features:**
- Creates ephemeral schema based on Dagster run_id
- Loads all files from manifest into a single `raw_data` table
- Standardizes geometry column name to "geom" using GDAL layer creation options
- Converts S3 paths (`s3://`) to GDAL virtual file system paths (`/vsis3/`)
- Supports multiple files per manifest with append semantics and schema validation

**Input:** Manifest dict with `batch_id`, `files`, `metadata`
**Output:** Schema info dict with `schema`, `manifest`, `tables`, `run_id`

**Resources Required:** `gdal`, `postgis`, `minio`

**Testing:** Unit tests in `tests/unit/ops/test_load_op.py`

#### spatial_transform (`etl_pipelines/ops/transform_op.py`)

**Status:** ✅ Implemented

Executes spatial transformations in PostGIS ephemeral schema using the **recipe-based transformation architecture**.

**Key Features:**
- Uses `RecipeRegistry` to get transformation recipe based on manifest `intent` field
- Executes transformation steps with table chaining (`raw_data` → `step_0` → `step_1` → `processed`)
- Default recipe includes:
  - CRS normalization to EPSG:4326
  - Geometry simplification (preserves topology)
  - Spatial index creation (GIST)
- Computes spatial bounds for metadata (may be None for empty geometry)
- Handles both transform steps (create new tables) and index steps (operate on existing tables)

**Recipe-Based Architecture:**
The op uses the recipe-based transformation system from `libs/transformations`:
1. Reads manifest `intent` field (defaults to `"ingest_vector"` if not specified)
2. Gets recipe from `RecipeRegistry.get_vector_recipe(intent, geom_column)`
3. Executes steps sequentially with table chaining
4. Renames final table to `processed` for downstream operations

**Table Chaining Pattern:**
```
raw_data → step_0 (CRS normalization) → step_1 (simplification) → processed (after index creation)
```

Index steps operate on the current table without creating new tables, while transform steps create new intermediate tables.

**Input:** Schema info dict from `load_to_postgis`
**Output:** Transform result dict with `schema`, `table`, `manifest`, `bounds`, `crs`, `run_id`

**Resources Required:** `postgis`

**Testing:** Unit tests in `tests/unit/ops/test_transform_op.py` (mocks `RecipeRegistry` and transformation steps)

**See Also:** `libs/transformations/CONTEXT.md` for detailed documentation on the recipe-based transformation architecture

#### export_to_datalake (`etl_pipelines/ops/export_op.py`)

**Status:** ✅ Implemented

Exports processed data from PostGIS to MinIO data lake and registers in MongoDB.

**Key Features:**
- Exports PostGIS data to GeoParquet format using ogr2ogr
- Calculates SHA256 content hash for deduplication
- Uploads to MinIO data lake with versioned paths
- Registers asset in MongoDB ledger
- **Automatically cleans up PostGIS schema** after export completes (success or failure)

**Schema Cleanup:**
The op uses a try/finally block to ensure the ephemeral PostGIS schema is always dropped after export completes, even if the export fails. This maintains the architectural law that PostGIS is transient compute only. The cleanup derives the schema name from the Dagster run_id using `RunIdSchemaMapping`.

**Ephemeral Schema Lifecycle Guarantees:**
Ephemeral schemas are automatically cleaned up in the following scenarios:
- **Export completion (success or failure)**: Schema is dropped in `export_to_datalake`'s finally block
- **Load failure**: Schema is dropped in `load_to_postgis`'s exception handler if ogr2ogr fails after schema creation
- **Transform failure**: Schema is dropped in `spatial_transform`'s exception handler if any transformation step fails

This ensures that PostGIS remains transient compute only and no schemas leak on failure paths.

**Input:** Transform result dict from `spatial_transform`
**Output:** Asset info dict with `asset_id`, `s3_key`, `dataset_id`, `version`, `content_hash`, `run_id`

**Resources Required:** `gdal`, `postgis`, `minio`, `mongodb`

**Testing:** 
- Unit tests in `tests/unit/ops/test_export_op.py`
- Integration tests verify schema cleanup in `tests/integration/test_schema_cleanup.py`

#### cleanup_postgis_schema (`etl_pipelines/ops/cleanup_op.py`)

**Status:** ✅ Implemented

Standalone op for dropping PostGIS schemas. While schema cleanup is automatically handled by `export_to_datalake`, this op is available for explicit cleanup if needed.

**Key Features:**
- Drops ephemeral schemas by schema name
- Handles missing schemas gracefully (idempotent)
- Logs cleanup failures without breaking the pipeline

**Input:** Schema info dict with `schema` key
**Output:** None (void op)

**Resources Required:** `postgis`

**Testing:** 
- Unit tests in `tests/unit/ops/test_cleanup_op.py`
- Integration tests in `tests/integration/test_schema_cleanup.py`

**Note:** The cleanup hooks (`cleanup_schema_on_success`, `cleanup_schema_on_failure`) are defined but not currently used, as cleanup is handled directly in `export_to_datalake`'s finally block for better reliability.

### Implemented Sensors

#### ManifestSensor (`etl_pipelines/sensors/manifest_sensor.py`)

**Status:** ✅ Implemented (Phase 3)

Polls MinIO landing zone for new manifest files and triggers ingestion jobs.

**Key Features:**
- Polls `s3://landing-zone/manifests/` every 30 seconds
- Uses sensor cursor to track processed manifests
- Validates manifests against `Manifest` Pydantic model
- Handles validation errors gracefully (logs, doesn't crash)
- Yields `RunRequest` with manifest data as run config

**Configuration:**
- Poll interval: 30 seconds (configurable via `minimum_interval_seconds`)
- Default status: RUNNING (starts automatically)

**Error Handling:**
- MinIO connection errors → SkipReason (retries on next evaluation)
- Invalid manifest JSON → Logged, skipped, added to cursor (only tried once)
- Individual manifest errors don't stop processing of other manifests
- All manifests (valid or invalid) are only processed once

**Cursor Management:**
- Format: Comma-separated list of manifest keys
- Tracks all processed manifests (valid and invalid) to prevent duplicate runs
- Cursor persists across sensor evaluations
- Stored in Dagster's metadata database (PostgreSQL)
- **Cursor Growth:** Cursor grows indefinitely as manifests are processed. This is acceptable as:
  - Each manifest key is typically <100 characters
  - Even 10,000 manifests = ~1MB string (negligible)
  - Cursor is stored in PostgreSQL, which handles large text fields efficiently
  - No cleanup mechanism needed unless processing millions of manifests

**Retry Mechanism:**

Manifests are only processed once (even if invalid). The current retry approach and future options are documented below.

**Current Approach (Phase 3):**
- **Re-upload with new key**: Upload manifest with a different key (e.g., `manifests/batch_001_retry.json`)
  - Simplest approach, no code changes needed
  - Works immediately
  - User manually re-uploads with new filename

**Future Retry Options (For Future Implementation):**

1. **Retry Prefix/Folder** (Recommended for Phase 4+)
   - Sensor checks `manifests/retry/` prefix separately, bypasses cursor check
   - User moves manifest from `manifests/batch_001.json` to `manifests/retry/batch_001.json`
   - More automated, better UX
   - Requires code changes to sensor

2. **MongoDB Status Check** (Recommended for Phase 4+)
   - Sensor checks MongoDB manifest status, allows retry if status is "failed" or "pending"
   - User updates MongoDB manifest status, then re-uploads
   - Most integrated with ledger, tracks retry history
   - Requires MongoDB integration in sensor

3. **Manual Job Trigger** (Always Available)
   - User manually triggers `ingest_job` via Dagster UI/API with run config
   - Full control, bypasses sensor entirely
   - Requires Dagster UI/API access

**Implementation Notes:**
- Retry options 1 and 2 should be implemented in future phases based on user feedback
- Option 3 is always available and doesn't require code changes
- Current approach (re-upload with new key) is sufficient for Phase 3

**Testing:**
- Unit tests: `tests/unit/sensors/test_manifest_sensor.py` (mock `MinIOResource` and `SensorEvaluationContext`)
- Integration tests: Future (Phase 4+) - test against real MinIO instance

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
│              │                               │               │
│              │                    Recipe-Based Transform     │
│              │                    (libs/transformations)     │
│              │                               │               │
│              └───────────────┬───────────────┘              │
│                              │                               │
│                              ▼                               │
│                        MongoDB (Ledger)                      │
└─────────────────────────────────────────────────────────────┘
```

**Recipe-Based Transformation Integration:**
The `spatial_transform` op integrates with the recipe-based transformation architecture from `libs/transformations`:
- Reads manifest `intent` to select transformation recipe
- Executes transformation steps via `RecipeRegistry`
- Generates and executes PostGIS SQL within ephemeral schemas
- Maintains single-geometry-column contract throughout transformation chain

## Configuration Requirements

### Environment Variables (Required)

| Variable | Description | Used By |
|----------|-------------|---------|
| `DAGSTER_POSTGRES_HOST` | Dagster DB hostname | Dagster instance |
| `DAGSTER_POSTGRES_USER` | Dagster DB username | Dagster instance |
| `DAGSTER_POSTGRES_PASSWORD` | Dagster DB password | Dagster instance |
| `DAGSTER_POSTGRES_DB` | Dagster DB name | Dagster instance |
| `MINIO_ENDPOINT` | MinIO server endpoint (host:port) | MinIO, GDAL resources |
| `MINIO_ROOT_USER` | MinIO access key | MinIO, GDAL resources |
| `MINIO_ROOT_PASSWORD` | MinIO secret key | MinIO, GDAL resources |
| `MONGO_CONNECTION_STRING` | MongoDB connection URI | MongoDB resource |
| `POSTGRES_HOST` | PostGIS hostname | PostGIS resource |
| `POSTGRES_USER` | PostGIS username | PostGIS resource |
| `POSTGRES_PASSWORD` | PostGIS password | PostGIS resource |
| `GDAL_DATA` | GDAL data files path (optional) | GDAL resource |
| `PROJ_LIB` | PROJ library path (optional) | GDAL resource |

**Note on MONGO_CONNECTION_STRING:** If not set directly, it can be built from individual MongoDB env vars using the format: `mongodb://[username]:[password]@[host]:[port]/[database]?authSource=[auth_source]`. See `MongoSettings.connection_string` in `libs.models.config` for the exact format.

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
- Current pins (Dec 2025): `dagster==1.12.5`, `dagster-webserver==1.12.5`, `dagster-postgres==0.28.5`, `dagster-aws==0.28.5`. Update together to keep compatibility.
- The `user-code` container requires `dagster-postgres` and `DAGSTER_POSTGRES_*` environment variables to properly initialize RPC storage/config, even if it doesn't host the DB itself.

