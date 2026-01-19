# libs/models/ — Agent Guide

## What this directory is / owns

This package contains **all Pydantic v2 models** used by the platform: manifests, assets, spatial types, and runtime settings.
It is the application-level validation layer and the contract between ingestion, orchestration, and persistence.

## Key invariants / non-negotiables

- **Pydantic v2 only**: use `model_config`, `model_dump`, `@field_validator` (no v1 patterns like `class Config` / `.dict()` / `@validator`).
- **Strict, explicit typing**: prefer clear types over implicit coercions.
- **Ledger alignment**: models that represent MongoDB documents must match Mongo init/schema expectations.
- **Unified status semantics**: `ManifestStatus` and `RunStatus` use the same values: `running`, `success`, `failure`, `canceled`.
- **HumanMetadataMixin contract**: Both `ManifestMetadata` and `AssetMetadata` inherit from `HumanMetadataMixin` which enforces 6 required fields: `title`, `description`, `keywords`, `source`, `license`, `attribution`. Empty strings are allowed.
- **Factory method for metadata propagation**: Always use `AssetMetadata.from_manifest_metadata()` in ops to ensure consistent field copying with defensive copies of mutable fields.
- **Manifest contract is disciplined**: `FileEntry` forbids extras (no uploader CRS); `ManifestMetadata` inherits human fields from mixin, plus allows `project` (now optional), `tags` (primitive scalars), and structured `join_config`. `ManifestRecord` does NOT store `dagster_run_id` (runs are tracked separately).
- **Intent/type coherence**: `Manifest` enforces that `intent="ingest_tabular"` requires all files to be `type="tabular"`, `intent="join_datasets"` requires `files=[]` with both `spatial_asset_id` and `tabular_asset_id` in join_config, and spatial intents forbid tabular files.
- **Asset kind discrimination**: `Asset` uses `kind` field (spatial/tabular/joined) to determine CRS/bounds requirements. Tabular assets have `crs=None` and `bounds=None`.
- **Kind-specific metadata enforcement**: Spatial/joined assets **must** have `metadata.geometry_type`; tabular/spatial/joined assets **must** have `metadata.column_schema`. Missing these fields for those kinds results in a `ValueError` during final validation.
- **Run linking via ObjectId**: `Asset.run_id` and lineage records use MongoDB ObjectId strings to reference the `runs` collection, NOT the raw Dagster run ID string.
- **Stable env var aliases**: settings fields must map to the env vars used in `docker-compose.yaml`.
- **JSON Schema is a public interface**: `ManifestCreateRequest.model_json_schema()` is consumed by the webapp for client-side validation. Changes to field names, required/optional status, or enum values will affect the webapp forms. Update `tests/unit/webapp/test_manifest_schema.py` when modifying manifest models.

## Entry points / key files

- `base.py`: `HumanMetadataMixin` - shared human-readable metadata fields
- `manifest.py`: ingestion manifest schema + runtime tracking variants
- `asset.py`: asset registry models (content hash, s3 keys, bounds, kind, `ColumnInfo`, etc.)
- `run.py`: run tracking models (dagster_run_id, batch_id, status, asset_ids)
- `activity.py`: audit logging models (ActivityLog, ActivityAction enum)
- `spatial.py`: CRS / bounds / enums (FileType, OutputFormat) and validators
- `config.py`: `pydantic-settings` models (env var aliases + computed connection strings)
- `__init__.py`: public exports

## How to work here

- **Add/modify a model**:
  - implement the model with v2 validators
  - export it from `__init__.py` if it’s part of the public surface
  - add/extend unit tests under `tests/unit/`

- **Update settings/env vars**:
  - update `config.py` aliases
  - update `docker-compose.yaml` and any runtime usage to match
  - keep docs pointing to code/compose rather than hardcoding values

## Common tasks

- **Add a new manifest field**:
  - update `manifest.py`
  - update any sensor/op parsing that depends on it
  - add unit tests and (if needed) integration tests

- **Add a new settings field**:
  - update the relevant `BaseSettings` class in `config.py`
  - ensure `docker-compose.yaml` sets the variable in `user-code` (and webserver/daemon if applicable)

## Environment variables (canonical)

These are the env vars read by `libs/models/config.py` (via `validation_alias`). Defaults shown are code defaults.

- **MinIO** (`MinIOSettings`)
  - `MINIO_ENDPOINT` (required in dev; must be `host:port` **without** scheme, e.g., `minio:9000` - no `http://` or `https://`)
  - `MINIO_ROOT_USER` (required)
  - `MINIO_ROOT_PASSWORD` (required)
  - `MINIO_USE_SSL` (default: `false`)
  - `MINIO_LANDING_BUCKET` (default: `landing-zone`)
  - `MINIO_LAKE_BUCKET` (default: `data-lake`)

- **MongoDB** (`MongoSettings`)
  - `MONGO_HOST` (default: `mongodb`)
  - `MONGO_PORT` (default: `27017`)
  - `MONGO_INITDB_ROOT_USERNAME` (required)
  - `MONGO_INITDB_ROOT_PASSWORD` (required)
  - `MONGO_DATABASE` (default: `spatial_etl`)
  - `MONGO_AUTH_SOURCE` (default: `admin`)

  Notes:
  - Docker init uses `MONGO_INITDB_*`.
  - App settings also use `MONGO_*` (including `MONGO_INITDB_ROOT_USERNAME/PASSWORD` as aliases for credentials).

- **PostGIS** (`PostGISSettings`)
  - `POSTGRES_HOST` (default: `postgis`)
  - `POSTGRES_PORT` (default: `5432`)
  - `POSTGRES_USER` (required)
  - `POSTGRES_PASSWORD` (required)
  - `POSTGRES_DB` (default: `spatial_compute`)

- **Dagster metadata DB** (`DagsterPostgresSettings`)
  - `DAGSTER_POSTGRES_HOST` (required)
  - `DAGSTER_POSTGRES_PORT` (default: `5433`)
  - `DAGSTER_POSTGRES_USER` (required)
  - `DAGSTER_POSTGRES_PASSWORD` (required)
  - `DAGSTER_POSTGRES_DB` (required)

- **GDAL** (`GDALSettings`)
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_S3_ENDPOINT` (example in container: `http://minio:9000`; **includes** scheme, unlike `MINIO_ENDPOINT`)
  - `GDAL_DATA` (optional)
  - `PROJ_LIB` (optional)

## Testing / verification

- Unit tests live under `tests/unit/` (models + settings).
- Prefer testing validators via model instantiation (and direct validator calls only when appropriate).

## Links

- Root guide: `../../AGENTS.md`
- Config placeholder: `../../configs/AGENTS.md`
- MongoDB service: `../../services/mongodb/AGENTS.md`
