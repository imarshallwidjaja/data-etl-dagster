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
- **Manifest Router (Traffic Controller)**: `MANIFEST_ROUTER_ENABLED_LANES` (comma-separated; legacy `manifest_sensor` only uses `ingest`; defaults to `ingest` when unset)

Notes:
- Bucket names (`landing-zone`, `data-lake`) and DB names (`spatial_etl`, `spatial_compute`) are currently hardcoded in `etl_pipelines/definitions.py` to match architectural defaults.
- Manifest schema is documented in the repo-root guide: `../../AGENTS.md`.

### Asset partitioning (dataset_id)

The asset graph is **partitioned by `dataset_id`** using Dagster dynamic partitions.

- Definition lives in `etl_pipelines/partitions.py` as `dataset_partitions` (`DynamicPartitionsDefinition(name="dataset_id")`).
- Partition keys are **registered at runtime** by partitioned assets when they materialize.
- Partition key source (from the manifest): `metadata.tags.dataset_id` if present, else generated `dataset_{uuid12}`.

### Sensors (Legacy + Asset-based)

The platform uses **dedicated sensors** to ensure manifests trigger the correct lane (legacy op jobs vs asset jobs).

**Sensor responsibilities (target):**
- `manifest_sensor` (**LEGACY**) → triggers `ingest_job` for non-tabular, non-join intents
- `spatial_sensor` → triggers `spatial_asset_job` for `intent in {"ingest_vector", "ingest_raster"}` (materializes `raw_spatial_asset`)
- `tabular_sensor` → triggers `tabular_asset_job` for `intent == "ingest_tabular"` (materializes `raw_tabular_asset`)
- `join_sensor` → triggers `join_asset_job` for `intent == "join_datasets"` (materializes `joined_spatial_asset`; requires `metadata.join_config.spatial_asset_id` + `tabular_asset_id`)

**Note (asset architecture fix)**:
- The legacy `manifest_sensor` intentionally **skips** tabular/join intents (it marks them processed for itself, but does not archive them) so the dedicated asset sensors can process + archive them.

**Traffic Controller (`MANIFEST_ROUTER_ENABLED_LANES`)**:
- Controls whether the legacy `manifest_sensor` may launch `ingest_job`
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
- \"Most recent\" refers to **most recently processed by the sensor** (preserves processing order)

### Run Status Tracking

Run lifecycle is tracked in MongoDB via dedicated sensors:

**Run Status Sensors:**
- `manifest_run_failure_sensor` - Updates manifest status to `failure`/`canceled` when runs fail or are canceled
- `manifest_run_success_sensor` - Updates manifest status to `success` when runs complete successfully

**Run Document Lifecycle:**
1. `init_mongo_run_op` creates the run document with `status=running` at job start
2. Assets link to the run via `run_id` (MongoDB ObjectId)
3. Run status sensors update both `runs` and `manifests` collections on completion

**Key Fields:**
- `runs.dagster_run_id` - Links to Dagster run
- `runs.batch_id` - Links to manifest
- `runs.asset_ids` - ObjectIds of assets produced by this run
- `assets.run_id` - ObjectId reference to run document (NOT the Dagster run ID string)

**Status Values (Unified):**
Both manifests and runs use: `running`, `success`, `failure`, `canceled`

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
