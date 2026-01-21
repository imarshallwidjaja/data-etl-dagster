# services/minio — Agent Guide

## Scope
MinIO storage for raw uploads (landing zone) and processed outputs (data lake).

## Key invariants
- Users write to `landing-zone`; pipeline writes to `data-lake`.
- Manifests live under `landing-zone/manifests/` and trigger runs.
- Processed manifests are archived under `landing-zone/archive/`.

## References
- Env vars/config: `libs/models/AGENTS.md` and `docker-compose.yaml`
- MinIO resource: `services/dagster/etl_pipelines/resources/minio_resource.py`
