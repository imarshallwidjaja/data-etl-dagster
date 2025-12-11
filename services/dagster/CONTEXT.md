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

### Planned Resources

- **PostGISResource:** Connection pooling and ephemeral schema lifecycle management
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
- S3/MinIO access uses `dagster-aws` S3 resources pointed at the MinIO endpoint (S3-compatible); ensure credentials/endpoint match MinIO in env vars.

## Common Mistakes

- Version tracks differ: core Dagster (`dagster`, `dagster-webserver`) uses 1.x (e.g., 1.12.5); integrations (`dagster-postgres`, `dagster-aws`) use 0.28.x (e.g., 0.28.4). Pin explicitly to matching tracks to avoid resolver/runtime issues.
- Current pins (Dec 2025): `dagster==1.12.5`, `dagster-webserver==1.12.5`, `dagster-postgres==0.28.4`, `dagster-aws==0.28.4`. Update together to keep compatibility.

