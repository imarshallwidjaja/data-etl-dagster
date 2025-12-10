# Service Context: MongoDB Metadata Ledger

## Overview

MongoDB serves as the **absolute registry** and metadata ledger for the Spatial ETL Pipeline. If a dataset is not recorded in MongoDB, it does not "exist" in the platform.

## Responsibilities

1. **Asset Registry:** Track all datasets, their versions, and lineage.
2. **Schema Validation:** Enforce document structure via MongoDB JSON Schema.
3. **Provenance Tracking:** Store `run_id`, `content_hash`, and transformation history.
4. **Cross-Reference:** Link MongoDB documents to S3 objects via `s3_key`.

## Architecture Components

### Containers

| Container | Purpose | Port |
|-----------|---------|------|
| `mongodb` | Document database | 27017 |

### Collection Structure

```
spatial_etl (database)
├── assets                  # Core asset registry
├── manifests              # Ingested manifest records
├── runs                   # ETL run metadata
└── lineage                # Asset transformation graph
```

## Input/Output Contracts

### Document Schemas

#### Assets Collection

```json
{
  "_id": "ObjectId",
  "s3_key": "data-lake/dataset_001/v1/data.parquet",
  "dataset_id": "dataset_001",
  "version": 1,
  "content_hash": "sha256:abc123...",
  "dagster_run_id": "run_12345",
  "format": "geoparquet",
  "crs": "EPSG:4326",
  "bounds": {
    "minx": -180,
    "miny": -90,
    "maxx": 180,
    "maxy": 90
  },
  "metadata": {
    "title": "Dataset Title",
    "description": "...",
    "source": "...",
    "license": "..."
  },
  "created_at": "ISODate",
  "updated_at": "ISODate"
}
```

#### Manifests Collection

```json
{
  "_id": "ObjectId",
  "batch_id": "batch_XYZ",
  "uploader": "user_id",
  "intent": "ingest_satellite_raster",
  "files": [...],
  "status": "completed|failed|processing",
  "dagster_run_id": "run_12345",
  "ingested_at": "ISODate",
  "completed_at": "ISODate"
}
```

### Linking Strategy

```
┌─────────────────┐         ┌─────────────────┐
│   MongoDB Doc   │◄───────►│   S3 Object     │
├─────────────────┤         ├─────────────────┤
│ s3_key: "..."   │────────►│ Key: "..."      │
│                 │◄────────│ x-amz-meta-     │
│ _id: ObjectId   │         │ mongodb-doc-id  │
└─────────────────┘         └─────────────────┘
```

## Relation to Global Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    MongoDB (This Service)                     │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│   Dagster ──────► Log Lineage ──────► MongoDB                │
│                                           │                   │
│   MinIO ────────► Object Created ────────►│                   │
│                                           │                   │
│   API/Users ◄─── Query Metadata ◄────────┘                   │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

## Configuration Requirements

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MONGO_INITDB_ROOT_USERNAME` | Admin username | mongo |
| `MONGO_INITDB_ROOT_PASSWORD` | Admin password | mongo_password |
| `MONGO_INITDB_DATABASE` | Default database | spatial_etl |

### JSON Schema Validation

Schema validation is configured in `init/` scripts to enforce document structure at the database level.

## Development Notes

- Access MongoDB via:
  ```bash
  mongosh "mongodb://mongo:mongo_password@localhost:27017/spatial_etl?authSource=admin"
  ```
- Use MongoDB Compass for GUI access: `mongodb://localhost:27017`
- Indexes should be created for: `s3_key`, `dataset_id`, `content_hash`, `dagster_run_id`

