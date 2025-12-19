# services/minio/ — Agent Guide

## What this directory is / owns

This directory documents the MinIO storage role in the platform.
MinIO is the **vault** for raw uploads (landing zone) and processed outputs (data lake).

> **Note**: This directory only contains this guide. MinIO configuration lives in `docker-compose.yaml`.

## Key invariants / non-negotiables

- **Ingestion contract**: users write to `landing-zone`; pipeline writes to `data-lake`.
- **Manifests trigger runs**: manifests live under `landing-zone/manifests/`.
- **No direct writes to the lake** from users/upstreams.
- **Processed manifests are archived**: `manifests/batch.json` → `archive/manifests/batch.json`

## Bucket structure

```
landing-zone/                    # Raw uploads (ephemeral)
├── manifests/                   # Manifest JSONs (trigger sensors)
│   └── batch_123.json
├── archive/                     # Processed manifests
│   └── manifests/
│       └── batch_123.json
└── e2e/                        # E2E test uploads
    └── <batch_id>/
        └── data.json

data-lake/                       # Processed outputs (permanent)
└── <dataset_id>/
    └── v<version>/
        └── data.parquet
```

## Entry points / key files

- Dev configuration: `../../docker-compose.yaml`
- MinIO resource: `../dagster/etl_pipelines/resources/minio_resource.py`
- Bucket init: `minio-init` container in docker-compose

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ENDPOINT` | `minio:9000` | Host:port (NO scheme) |
| `MINIO_ROOT_USER` | (required) | Access key |
| `MINIO_ROOT_PASSWORD` | (required) | Secret key |
| `MINIO_USE_SSL` | `false` | Enable HTTPS |
| `MINIO_LANDING_BUCKET` | `landing-zone` | Raw uploads bucket |
| `MINIO_LAKE_BUCKET` | `data-lake` | Processed outputs bucket |

## How to work here

- Bucket creation in dev is handled by the `minio-init` container (see `docker-compose.yaml`).
- Bucket names are configurable via env vars, but pipeline code currently assumes defaults.
- Access MinIO UI at `http://localhost:9001` (dev) with configured credentials.

## Common tasks

- **Change bucket names**:
  - Update `docker-compose.yaml` (`MINIO_LANDING_BUCKET`, `MINIO_LAKE_BUCKET`)
  - Update any hardcoded bucket references in Dagster resources

- **Manually upload a manifest** (dev/testing):
  ```bash
  mc alias set local http://localhost:9000 admin admin
  mc cp my-manifest.json local/landing-zone/manifests/
  ```

- **Check archived manifests**:
  ```bash
  mc ls local/landing-zone/archive/manifests/
  ```

## Testing / verification

- Integration: `pytest -m integration tests/integration/test_minio.py`
- Unit (resource): `pytest tests/unit/test_minio_resource.py`

## Links

- Root guide (manifest protocol): `../../AGENTS.md`
- Dagster orchestration: `../dagster/AGENTS.md`
- MinIO resource code: `../dagster/etl_pipelines/resources/minio_resource.py`
