# Spatial Data ETL Pipeline

An automated, containerized ETL pipeline specialized for processing spatial data (vector and raster) using Dagster, PostGIS, MinIO, and MongoDB.

## Overview

This platform processes spatial data through a strict manifest-based ingestion protocol:

1. **Upload** raw files to MinIO landing zone
2. **Trigger** processing via manifest JSON
3. **Transform** data using PostGIS as a compute engine with recipe-based transformations (geometry column standardized to `geom`)
4. **Store** processed GeoParquet in the data lake
5. **Track** lineage in MongoDB ledger

**Recipe-Based Transformations:** The pipeline uses a recipe-based transformation architecture that maps manifest `intent` fields to ordered lists of transformation steps (CRS normalization, geometry simplification, spatial indexing). See `CONTEXT.md` Section 10 for details.

**Geometry Column Contract:** In PostGIS compute schemas, vector geometry column is standardized to `geom`, and transforms preserve a single geometry column. Bounds may be empty for empty datasets.

## Quick Start

### Prerequisites

- Docker & Docker Compose v2+
- Git

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd data-etl-dagster

# Copy environment template
copy env.example .env

# Start all services
docker compose up -d

# Access the Dagster UI
# Open http://localhost:3000
```

### Service Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| Dagster UI | http://localhost:3000 | - |
| MinIO Console | http://localhost:9001 | See .env |
| MinIO API | http://localhost:9000 | See .env |
| MongoDB | localhost:27017 | See .env |
| PostGIS | localhost:5432 | See .env |

## Usage

### Triggering Data Ingestion

The pipeline uses a manifest-based ingestion protocol. To trigger processing, upload a `manifest.json` file to the MinIO landing zone.

#### Step 1: Upload Your Data Files

First, upload your raw spatial data files (GeoTIFF, Shapefile, GeoJSON, etc.) to the landing zone bucket:

```bash
# Using MinIO client (mc)
mc cp your-data.tif minio/landing-zone/batch_001/your-data.tif

# Or using AWS CLI (configured for MinIO)
aws --endpoint-url http://localhost:9000 s3 cp your-data.tif s3://landing-zone/batch_001/your-data.tif
```

**Note:** Organize files in subdirectories (e.g., `batch_001/`) to keep batches separate.

#### Step 2: Create and Upload Manifest

Create a `manifest.json` file describing your data. The manifest format is currently being standardized, but the following structure is supported:

**Example: Vector Data (Building Footprints)**
```json
{
  "batch_id": "batch_buildings_001",
  "uploader": "user_123",
  "intent": "ingest_building_footprints",
  "files": [
    {
      "path": "s3://landing-zone/batch_buildings_001/buildings.geojson",
      "type": "vector",
      "format": "GeoJSON",
      "crs": "EPSG:4326"
    }
  ],
  "metadata": {
    "project": "BUILDINGS_DEMO",
    "description": "Building footprints with intent-driven heavy simplification"
  }
}
```

**Example: Vector Data (Default Recipe)**
```json
{
  "batch_id": "batch_vector_001",
  "uploader": "user_123",
  "intent": "ingest_vector",
  "files": [
    {
      "path": "s3://landing-zone/batch_vector_001/data.geojson",
      "type": "vector",
      "format": "GeoJSON",
      "crs": "EPSG:4326"
    }
  ],
  "metadata": {
    "project": "ALPHA",
    "description": "User supplied context"
  }
}
```

**Manifest Fields:**
- `batch_id` (required): Unique identifier for this batch
- `uploader` (required): User or system identifier
- `intent` (required): Processing intent that determines the transformation recipe:
  - `ingest_vector`: Default vector recipe (CRS normalization, light simplification, spatial indexing)
  - `ingest_building_footprints`: Building footprints recipe with stronger geometry simplification (0.001° tolerance ≈ 111m at equator) for visibly simplified outlines
  - `ingest_road_network`: Road network recipe (currently uses default recipe, can be customized)
  - Unknown intents fall back to the default recipe
- `files` (required): Array of file entries, each with:
  - `path`: S3 path to the file (must start with `s3://landing-zone/`)
  - `type`: File type (`raster` or `vector`)
  - `format`: Input format (e.g., `GTiff`, `GPKG`, `SHP`, `GeoJSON`)
  - `crs`: Coordinate Reference System (e.g., `EPSG:4326`)
- `metadata` (required): User-supplied metadata with:
  - `project`: Project identifier
  - `description`: Optional description

**Intent-Based Transformations:**
The pipeline uses a recipe-based transformation architecture where the `intent` field determines which transformation steps are applied. Each recipe includes:
- CRS normalization to EPSG:4326
- Geometry simplification (tolerance varies by intent)
- Spatial index creation (GIST)

See `libs/transformations/CONTEXT.md` for detailed documentation on the recipe system.

**Note:** The manifest schema is subject to change as the system evolves. Check `CONTEXT.md` for the latest schema definition.

Upload the manifest to the `manifests/` prefix:

```bash
# Using MinIO client
mc cp manifest.json minio/landing-zone/manifests/batch_001.json

# Or using AWS CLI
aws --endpoint-url http://localhost:9000 s3 cp manifest.json s3://landing-zone/manifests/batch_001.json
```

#### Step 3: Monitor Processing

The manifest sensor polls the `manifests/` prefix every 30 seconds. Once your manifest is detected:

1. **Sensor Detection** (within 30 seconds):
   - The manifest sensor detects the new JSON file
   - Validates the manifest against the schema
   - If valid, triggers the ingestion job

2. **Job Execution** (Load → Transform → Export):
   - **Load**: Files are loaded from MinIO landing zone to PostGIS ephemeral schema using GDAL ogr2ogr
   - **Transform**: Spatial transformations are applied using recipe-based architecture (CRS normalization, geometry simplification, spatial indexing)
   - **Export**: Processed data is exported to GeoParquet format and uploaded to MinIO data lake
   - **Register**: Asset metadata and lineage are recorded in MongoDB ledger
   - **Cleanup**: PostGIS ephemeral schema is automatically dropped after export

3. **Monitor Progress**:
   - View job status in Dagster UI: http://localhost:3000
   - Check logs for processing details
   - Verify results in the data lake bucket

### What Happens After Upload

Once a manifest is uploaded, the following sequence occurs:

1. **Sensor Polling**: The manifest sensor checks for new manifests every 30 seconds
2. **Validation**: The manifest is validated against the Pydantic schema
3. **Job Trigger**: If valid, a Dagster run is created with the manifest data passed as an op input
4. **Processing**: The ingestion job executes the full ETL pipeline:
   - Loads spatial data from landing zone to PostGIS ephemeral schema
   - Transforms data using recipe-based transformations (based on manifest `intent`)
   - Exports processed GeoParquet to data lake
   - Registers asset in MongoDB ledger
   - Cleans up PostGIS ephemeral schema
5. **Tracking**: The manifest is marked as processed in the sensor cursor (prevents duplicate runs)

**Important Notes:**
- Each manifest is processed **only once** (even if invalid)
- Invalid manifests are logged but not retried automatically
- Processing status can be viewed in the Dagster UI

### Retrying Failed Ingestion

If ingestion fails or you need to reprocess a manifest, you have several options:

#### Option 1: Re-upload with New Key (Recommended for Phase 3)

The simplest approach is to upload the manifest with a different filename:

```bash
# Original manifest
s3://landing-zone/manifests/batch_001.json

# Retry with new key
s3://landing-zone/manifests/batch_001_retry.json
```

**Why this works:** The sensor tracks processed manifests by their S3 key. A new key is treated as a new manifest.

**When to use:** 
- Quick retry after fixing manifest errors
- Reprocessing with corrected data
- Testing different processing parameters

#### Option 2: Manual Job Trigger (Always Available)

You can manually trigger the ingestion job via the Dagster UI:

1. Navigate to http://localhost:3000
2. Go to the "Jobs" section
3. Select `ingest_job`
4. Click "Launch Run"
5. Provide the manifest data in the run config:

```yaml
ops:
  load_to_postgis:
    inputs:
      manifest:
        value:
          batch_id: "batch_001"
          uploader: "user_123"
          intent: "ingest_vector"
          files:
            - path: "s3://landing-zone/batch_001/your-data.geojson"
              type: "vector"
              format: "GeoJSON"
              crs: "EPSG:4326"
          metadata:
            project: "ALPHA"
            description: "Retry after fix"
```

**Note:** The manifest is passed as an op input (not op config). The `manifest_key` is not required for manual triggers but can be added as a tag if needed for traceability.

**When to use:**
- Full control over run configuration
- Testing specific scenarios
- Bypassing sensor entirely

#### Option 3: Future Retry Mechanisms (Phase 4+)

Future enhancements will provide automated retry mechanisms:

- **Retry Prefix/Folder**: Move manifest to `manifests/retry/` to bypass cursor check
- **MongoDB Status Check**: Update manifest status in MongoDB to allow retry
- **Automatic Retry**: Failed runs automatically retry based on status

These options will be documented when implemented.

### Troubleshooting

**Manifest not detected:**
- Verify the file is in `s3://landing-zone/manifests/` prefix
- Ensure the file has a `.json` extension
- Check sensor status in Dagster UI (should be RUNNING)
- Wait up to 30 seconds for sensor polling interval

**Manifest validation errors:**
- Check Dagster logs for validation error details
- Verify all required fields are present
- Ensure S3 paths are correct and files exist
- Check CRS format (must be valid EPSG code, WKT, or PROJ string)

**Job not executing:**
- Check Dagster UI for run status
- Verify resources (MinIO, MongoDB, PostGIS) are accessible
- Review job logs for errors
- Ensure the manifest was successfully validated

## Architecture

```mermaid
graph TD
    subgraph "Docker Network"
        %% Dagster Components
        Daemon[Dagster Daemon]
        Sensor[Dagster Sensor]
        CodeLoc["User Code Container (Python + GDAL Libs)"]
        
        %% Storage & Compute
        Landing[(MinIO: Landing Zone)]
        Lake[(MinIO: Data Lake)]
        Mongo[(MongoDB: Ledger)]
        PostGIS[(PostGIS: Compute)]
        
        %% Data Flow
        User -->|1. Upload Files + Manifest| Landing
        Landing -->|2. Manifest detected| Sensor
        Sensor -->|3. Signal run| Daemon
        Daemon -->|4. Launch Run| CodeLoc
        CodeLoc -->|5. Read Raw Data| Landing
        CodeLoc -->|6. Spatial Ops - SQL| PostGIS
        CodeLoc -->|7. Write GeoParquet| Lake
        CodeLoc -->|8. Log Lineage| Mongo
    end
```

## Repository Structure

```
data-etl-dagster/
├── CONTEXT.md              # Global architecture context
├── docker-compose.yaml     # Service orchestration
├── services/
│   ├── dagster/           # Dagster orchestrator
│   ├── minio/             # Object storage config
│   ├── mongodb/           # Metadata store config
│   └── postgis/           # Compute engine config
├── libs/                   # Shared Python libraries (installable package)
│   ├── pyproject.toml     # Package definition
│   ├── __init__.py        # Package root
│   ├── spatial_utils/     # GDAL wrappers
│   ├── transformations/   # Recipe-based transformation steps
│   └── models/            # Pydantic schemas
└── configs/               # Configuration templates
```

## Documentation

Each component has its own `CONTEXT.md` with detailed documentation:

- [Global Context](./CONTEXT.md) - Architecture and philosophy
- [Dagster](./services/dagster/CONTEXT.md) - Orchestration layer
- [MinIO](./services/minio/CONTEXT.md) - Object storage
- [MongoDB](./services/mongodb/CONTEXT.md) - Metadata ledger
- [PostGIS](./services/postgis/CONTEXT.md) - Compute engine
- [Spatial Utils](./libs/spatial_utils/CONTEXT.md) - GDAL wrappers
- [Transformations](./libs/transformations/CONTEXT.md) - Recipe-based transformation architecture
- [Models](./libs/models/CONTEXT.md) - Data schemas

## Development

### Local Development

```bash
# Start services in development mode
docker compose up -d

# View logs
docker compose logs -f dagster-webserver

# Rebuild after code changes
docker compose up -d --build user-code
```

### Testing

The project includes both unit tests (no services required) and integration tests (requires Docker stack).

#### Prerequisites

```bash
# Install test dependencies
pip install -r requirements-test.txt
```

#### Unit Tests

Unit tests validate Pydantic models and business logic without requiring running services:

```bash
# Run unit tests only
pytest tests/unit -v
```

#### Integration Tests

Integration tests verify connectivity and basic operations against running services. These require the Docker stack to be running.

**1. Start the Docker stack:**

```bash
# Start all services required for integration tests
docker compose -f docker-compose.yaml up -d --build \
  dagster-webserver dagster-daemon user-code minio minio-init mongodb postgis dagster-postgres
```

**2. Wait for services to be ready:**

```bash
# Wait for all services to become healthy
python scripts/wait_for_services.py
```

This script polls each service (MinIO, MongoDB, PostGIS, Dagster) until they're ready or timeout is reached.

**3. Run integration tests:**

```bash
# Run integration tests
pytest -m integration tests/integration -v
```

**4. Stop the Docker stack:**

```bash
# Stop and remove containers and volumes
docker compose -f docker-compose.yaml down -v
```

#### Running All Tests

```bash
# Run unit tests first
pytest tests/unit -v

# Then run integration tests (if Docker stack is running)
pytest -m integration tests/integration -v
```

#### Test Coverage

**Unit Tests** (`tests/unit/`) - No external dependencies required (189 tests):

- **Models & Validation** (`test_models.py` - 55 tests):
  - CRS validation (EPSG, WKT, PROJ formats)
  - Bounds validation
  - Manifest validation and status tracking
  - Asset registry models
  - Content hash validation
  - Configuration settings loading
  - S3 path/key validation

- **Resources**:
  - `test_minio_resource.py` (16 tests) - MinIO resource operations
    - Manifest listing and filtering
    - Manifest download and JSON parsing
    - Archive operations (copy and delete)
    - File upload to data lake with content-type inference
    - Presigned URL generation for GDAL
  - `test_mongodb_resource.py` (6 tests) - MongoDB resource operations
    - Document CRUD operations
    - Manifest and asset registry operations
  - `test_postgis_resource.py` (21 tests) - PostGIS resource operations
    - Ephemeral schema lifecycle management
    - SQL execution and transaction handling
    - Table existence checks and bounds calculation
  - `resources/test_gdal_resource.py` (20 tests) - GDAL resource operations
    - ogr2ogr command construction and execution
    - gdal_translate operations
    - ogrinfo and gdalinfo commands
    - Environment variable handling
    - Failure detection and result serialization

- **Transformations** (`libs/`):
  - `test_registry.py` (6 tests) - Recipe registry and intent mapping
  - `test_transformations_base.py` (4 tests) - Base transformation step classes
  - `test_vector_steps.py` (20 tests) - Vector transformation steps
    - CRS normalization
    - Geometry simplification
    - Spatial index creation
    - Geometry column contract validation

- **Operations** (`ops/`):
  - `test_load_op.py` (6 tests) - Data loading operations
  - `test_transform_op.py` (10 tests) - Spatial transformation operations
  - `test_export_op.py` (10 tests) - Data export operations
  - `test_cleanup_op.py` (9 tests) - Schema cleanup operations

- **Sensors** (`sensors/test_manifest_sensor.py` - 9 tests):
  - Manifest detection and validation
  - Cursor tracking and error handling
  - Run request generation

- **Utilities** (`test_schema_mapper.py` - 20 tests):
  - Schema name generation from run IDs
  - Run ID extraction from schema names
  - Round-trip validation

**Integration Tests** (`tests/integration/`) - Requires Docker stack (18 tests):

- `test_minio.py` (3 tests) - MinIO connectivity and read/write operations
- `test_mongodb.py` (3 tests) - MongoDB connectivity and CRUD operations
- `test_postgis.py` (4 tests) - PostGIS connectivity and spatial functions
- `test_dagster.py` (3 tests) - Dagster GraphQL API connectivity
- `test_gdal_health.py` (2 tests) - GDAL installation health check via Dagster
- `test_schema_cleanup.py` (3 tests) - Ephemeral schema lifecycle verification

**Total Coverage:** 189 unit tests + 18 integration tests = 207 tests

#### Continuous Integration

The project uses GitHub Actions with **smart path-based test execution**:

```
Trigger: Pull request or push to main/develop
         ↓
Detect changed files
         ↓
         ├─ Code changes? → Run unit + integration tests
         ├─ Infrastructure/Docker changes? → Run integration tests
         ├─ Docs-only changes? → Skip all tests
         └─ Manual dispatch? → Run all tests (escape hatch)
```

**Workflow file:** `.github/workflows/integration.yml`

**Benefits:**
- ⚡ Fast feedback: Unit tests run in ~5 seconds
- 💰 Resource efficient: Skip expensive Docker for model changes
- 🎯 Targeted: Only run tests relevant to changed files
- 🔄 Safe: Manual dispatch option runs full suite

See the workflow file for detailed path filter configuration.

## License

[Add license information]

