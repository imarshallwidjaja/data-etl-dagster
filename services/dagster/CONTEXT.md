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

All resources are configured via environment variables:

```python
# Example Resource Configuration
@resource
def minio_resource(context) -> MinIOResource:
    return MinIOResource(
        endpoint=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=os.environ.get("MINIO_USE_SSL", "false").lower() == "true"
    )
```

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

