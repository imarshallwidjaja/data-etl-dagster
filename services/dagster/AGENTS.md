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
- **Manifest Router (Traffic Controller)**: `MANIFEST_ROUTER_ENABLED_LANES` (comma-separated: `ingest,tabular,join`; defaults to `ingest` when unset)

Notes:
- Bucket names (`landing-zone`, `data-lake`) and DB names (`spatial_etl`, `spatial_compute`) are currently hardcoded in `etl_pipelines/definitions.py` to match architectural defaults.
- Manifest schema is documented in the repo-root guide: `../../AGENTS.md`.

### Manifest Sensor (Multi-Lane Router)

The `manifest_sensor` is a multi-lane router that routes manifests to different jobs based on the `intent` field.

**Lane Mapping:**
- `intent == "ingest_tabular"` → `tabular` lane → `ingest_tabular_job`
- `intent == "join_datasets"` → `join` lane → `join_asset_job` (requires `metadata.join_config`)
- All other intents → `ingest` lane → `ingest_job` (default)

**Traffic Controller (`MANIFEST_ROUTER_ENABLED_LANES`):**
- Controls which lanes may launch runs
- Comma-separated values: `ingest`, `tabular`, `join`
- Default when unset: `ingest` only
- If a manifest maps to a disabled lane, the sensor:
  - Logs that the lane is disabled
  - Marks the manifest as processed (one-shot)
  - Archives the manifest to `archive/{manifest_key}`
  - Yields no `RunRequest`

**Run Key Format:**
- Lane-prefixed to avoid collisions: `{lane}:{batch_id}` (e.g., `ingest:batch_001`, `tabular:batch_002`)

**Tags:**
- Standard tags: `batch_id`, `uploader`, `intent`, `manifest_key`
- Lane tag: `lane` (values: `ingest`, `tabular`, `join`)
- Archive tag: `manifest_archive_key` (value: `archive/{manifest_key}`)

**Manifest Archiving:**
- After processing (valid, invalid, or error), manifests are automatically archived via `minio.move_to_archive(manifest_key)`
- Archive path: `archive/{manifest_key}` (e.g., `archive/manifests/batch_001.json`)
- Archive failures are logged as warnings but do not prevent cursor updates

**Cursor Format:**
- Versioned JSON format: `{"v": 1, "processed_keys": [...], "max_keys": 500}`
- Migrates automatically from legacy comma-separated format
- Bounded to `MAX_CURSOR_KEYS` (500) to prevent unbounded growth
- `max_keys` is informational only; implementation ignores any persisted value
- "Most recent" refers to **most recently processed by the sensor** (preserves processing order)

### Retry semantics

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
