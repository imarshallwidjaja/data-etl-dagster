# services/minio/ — Agent Guide

## What this directory is / owns

This directory documents the MinIO storage role in the platform.
MinIO is the **vault** for raw uploads (landing zone) and processed outputs (data lake).

## Key invariants / non-negotiables

- **Ingestion contract**: users write to `landing-zone`; pipeline writes to `data-lake`.
- **Manifests trigger runs**: manifests live under `landing-zone/manifests/`.
- **No direct writes to the lake** from users/upstreams.

## Entry points / key files

- Dev configuration is in `../../docker-compose.yaml`.

## How to work here

- Bucket creation in dev is handled by the `minio-init` container (see `docker-compose.yaml`).
- Bucket names are configurable via env vars, but the pipeline currently assumes defaults unless configured otherwise.

## Common tasks

- **Change bucket names**:
  - update `docker-compose.yaml` (`MINIO_LANDING_BUCKET`, `MINIO_LAKE_BUCKET`)
  - update any code paths that assume `landing-zone` / `data-lake` (notably Dagster resources)

## Testing / verification

- Integration: `pytest -m integration tests/integration/test_minio.py`

## Links

- Root guide (manifest protocol): `../../AGENTS.md`
- Dagster orchestration: `../dagster/AGENTS.md`
