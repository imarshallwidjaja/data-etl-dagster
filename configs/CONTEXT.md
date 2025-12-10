# Directory Context: Configs

## Overview

This directory contains configuration templates and shared configuration files for the Spatial ETL Pipeline.

## Purpose

- Store environment-specific configuration templates
- Provide base configurations that can be extended
- Centralize configuration management

## Contents (Planned)

```
configs/
├── CONTEXT.md           # This file
├── logging.yaml         # Logging configuration template
├── sensors.yaml         # Sensor configuration
└── resources.yaml       # Resource definitions
```

## Relation to Global Architecture

Configuration files in this directory are used by:
- Dagster user code for resource configuration
- Docker Compose for service configuration

## Configuration Models

Configuration models are implemented in `libs/models/config.py` using `pydantic-settings`. These models load settings from environment variables with the following prefixes:

| Settings Class | Environment Prefix | Purpose |
|----------------|-------------------|---------|
| `MinIOSettings` | `MINIO_` | MinIO/S3 object storage connection |
| `MongoSettings` | `MONGO_` | MongoDB metadata ledger connection |
| `PostGISSettings` | `POSTGRES_` | PostGIS spatial compute engine (port 5432) |
| `DagsterPostgresSettings` | `DAGSTER_POSTGRES_` | Dagster internal metadata DB (port 5433) |

**Note:** `PostGISSettings` and `DagsterPostgresSettings` are separate databases:
- **PostGIS** (`POSTGRES_*`): Transient compute engine for spatial operations
- **Dagster Postgres** (`DAGSTER_POSTGRES_*`): Dagster's workflow execution metadata

See `libs/models/CONTEXT.md` for detailed model definitions and usage examples.

## Notes

- Environment-specific values should use environment variables
- Secrets should NEVER be stored in this directory
- Use `.env` files for local development secrets
- Configuration models use explicit `validation_alias` mappings to load from environment variables (see `libs/models/config.py`)

