# Service Context: MinIO Object Storage

## Overview

MinIO provides S3-compatible object storage for the Spatial ETL Pipeline. It serves as the "vault" for all spatial data files, both raw uploads and processed outputs.

## Responsibilities

1. **Landing Zone Storage:** Receive and store raw uploads from users/upstream systems.
2. **Data Lake Storage:** Store processed, versioned GeoParquet and Cloud Optimized GeoTIFF files.
3. **Manifest Storage:** Hold trigger manifests that initiate ETL pipelines.
4. **S3 API Compatibility:** Provide standard S3 API for all data operations.

## Architecture Components

### Containers

| Container | Purpose | Port |
|-----------|---------|------|
| `minio` | S3-compatible object storage | 9000 (API), 9001 (Console) |
| `minio-init` | One-shot bucket initialization | - |

### Bucket Structure

```
MinIO
├── landing-zone/           # Read/Write for Users (Ephemeral)
│   ├── manifests/          # Trigger manifests (JSON)
│   │   └── batch_*.json
│   └── batch_*/            # Raw upload folders
│       ├── file1.tif
│       ├── file2.shp
│       └── ...
│
└── data-lake/              # Read-Only for Users (Permanent)
    └── {dataset_id}/
        └── {version}/
            ├── data.parquet
            └── metadata.json
```

## Input/Output Contracts

### Inputs

- **Raw Files:** Any spatial format (TIFF, SHP, GPKG, GeoJSON, etc.)
- **Manifests:** JSON files following the schema in root `CONTEXT.md`

### Outputs

- **GeoParquet Files:** Processed vector data
- **Cloud Optimized GeoTIFF:** Processed raster data
- **Object Metadata:** Each S3 object includes `mongodb_doc_id` in user-metadata headers

### Object Metadata Headers

```
x-amz-meta-mongodb-doc-id: <ObjectId>
x-amz-meta-content-hash: <SHA256>
x-amz-meta-dagster-run-id: <run_id>
x-amz-meta-created-at: <ISO8601>
```

## Relation to Global Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                         Data Flow                             │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│   User ───► landing-zone/ ───► Dagster ───► data-lake/       │
│                  │                              │              │
│                  │                              │              │
│                  ▼                              ▼              │
│           (Raw uploads)              (Processed GeoParquet)   │
│                                                               │
│   Both buckets link to MongoDB via object metadata headers   │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

## Configuration Requirements

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MINIO_ROOT_USER` | Admin username | minio |
| `MINIO_ROOT_PASSWORD` | Admin password | minio_password |
| `MINIO_LANDING_BUCKET` | Landing zone bucket name | landing-zone |
| `MINIO_LAKE_BUCKET` | Data lake bucket name | data-lake |

### Access Policies (Future)

- **landing-zone:** Read/Write for authenticated users
- **data-lake:** Read-only for users, Write for ETL service account

## Development Notes

- Access the MinIO Console at `http://localhost:9001`
- Use AWS CLI or `mc` (MinIO Client) for command-line operations:
  ```bash
  mc alias set local http://localhost:9000 minio minio_password
  mc ls local/landing-zone/
  ```
- The `minio-init` container runs once to create buckets, then exits

