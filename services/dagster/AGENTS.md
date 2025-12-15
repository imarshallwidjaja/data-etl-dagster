# services/dagster/ — Agent Guide

## What this directory is / owns

This directory owns the **Dagster orchestration layer**: instance config, container images, and the ETL user-code location.
Dagster runs sensors/jobs that implement the platform’s Load → Transform → Export flow.

## Key invariants / non-negotiables

- **Register everything**: new assets/jobs/sensors/resources must be registered in `etl_pipelines/definitions.py`.
- **GDAL isolation**: heavy spatial deps live only in the `user-code` image (`Dockerfile.user-code`).
- **PostGIS is transient**: per-run ephemeral schemas must be cleaned up reliably.
- **MinIO bucket contract**: landing → lake (no direct writes to lake).

## Entry points / key files

- `etl_pipelines/` (Dagster code location)
  - `etl_pipelines/definitions.py` (the canonical registration point)
- `dagster.yaml` (Dagster instance config)
- `workspace.yaml` (code location wiring)
- `Dockerfile` / `Dockerfile.user-code` (images)
- Dependency pins:
  - `requirements.txt` (webserver/daemon)
  - `requirements-user-code.txt` (user-code)

## How to work here

- **Add a new asset/op/job/sensor**:
  - implement under `etl_pipelines/`
  - register it in `etl_pipelines/definitions.py`

- **Edit shared libs** (`libs/`): the `libs` package is installed at build time in the `user-code` image.
  - after changing `libs/`, rebuild: `docker compose build user-code`

- **Keep Dagster pins aligned**:
  - core packages (`dagster`, `dagster-webserver`) must stay on the same patch version
  - integration track (`dagster-aws`, `dagster-postgres`) must stay aligned with each other

## Runtime configuration

Most runtime config is set via env vars in `../../docker-compose.yaml`. Dagster resources are configured via `EnvVar(...)` in `etl_pipelines/definitions.py`.

Common env vars:

- **Dagster metadata DB**: `DAGSTER_POSTGRES_HOST`, `DAGSTER_POSTGRES_USER`, `DAGSTER_POSTGRES_PASSWORD`, `DAGSTER_POSTGRES_DB`
- **MinIO**: `MINIO_ENDPOINT` (must be `host:port` without scheme, e.g., `minio:9000`), `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`
- **MongoDB**: `MONGO_CONNECTION_STRING`
- **PostGIS**: `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

Notes:
- Bucket names (`landing-zone`, `data-lake`) and DB names (`spatial_etl`, `spatial_compute`) are currently hardcoded in `etl_pipelines/definitions.py` to match architectural defaults.
- Manifest schema is documented in the repo-root guide: `../../AGENTS.md`.

### Retry semantics (Option A)

- **Sensor is one-shot**: The manifest sensor uses a cursor to mark processed manifests, preventing infinite retries. Once a manifest is processed (valid or invalid), it is marked in the cursor and will not be retried automatically.
- **`batch_id` is immutable**: The `batch_id` field in manifests is globally unique and immutable. A new object key (different manifest filename) does not guarantee a new run when `run_key = batch_id`.
- **Retries via Dagster rerun/re-execute**: Failed runs should be retried using Dagster's built-in rerun/re-execute functionality in the UI. This is the supported retry path.

## Common tasks

- **Add a new resource**: implement under `etl_pipelines/resources/`, add it to `Definitions(resources=...)` in `etl_pipelines/definitions.py`, and add unit tests.
- **Change the manifest ingestion behavior**: update the sensor under `etl_pipelines/sensors/` and corresponding unit tests.
- **Extend transformations**: update `../../libs/transformations/` and adjust `etl_pipelines/ops/transform_op.py` only if the contract changes.

## Testing / verification

- Unit tests (fast): `pytest tests/unit`
- Integration tests (Docker required): `pytest -m integration tests/integration`

## Links

- Root guide: `../../AGENTS.md`
- Transformations: `../../libs/transformations/AGENTS.md`
- MinIO: `../minio/AGENTS.md`
- MongoDB: `../mongodb/AGENTS.md`
- PostGIS: `../postgis/AGENTS.md`
