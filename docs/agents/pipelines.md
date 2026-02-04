# Dagster Pipeline Notes

This summary describes behavior of the Dagster code location.

## Partitioning
- Assets are partitioned by `dataset_id` (dynamic partitions).
- Key source: `metadata.tags.dataset_id` or a generated `dataset_{uuid12}`.

## Join asset behavior
- `joined_spatial_asset` uses DuckDB (httpfs + spatial).
- Join is key-based (left/right/inner/full outer) with text coercion to match
  PostGIS semantics.
- Join outputs are GeoParquet with `geo` metadata merged from the spatial parent
  and validated before upload.

## Data objects
- **Blobs**: content-addressed raw bytes stored under `s3://data-lake/blobs/...`.
- **Artifacts**: per-upload raw/intermediate references that point to blobs (includes source path + bucket).
- **Assets**: versioned, queryable outputs produced by the pipeline.

## Sensor routing
- `spatial_sensor` -> `spatial_asset_job` (spatial intents)
- `tabular_sensor` -> `tabular_asset_job` (tabular intents)
- `join_sensor` -> `join_asset_job` (join intent)
- `manifest_sensor` handles legacy op-based ingestion

## Run tracking
- Run lifecycle lives in MongoDB `runs` collection.
- Assets and lineage reference Mongo `run_id` (ObjectId string).

## Raw source archival + reruns
- Raw uploads are archived as **artifacts** (per upload) that reference content-addressed **blobs** in
  `s3://data-lake/blobs/...`.
- Archive flow is hash-first, upload-second so dedup checks avoid memory-heavy hashing when a blob already exists.
- Reruns rewrite manifest file paths to blob locations, and tabular downloads resolve blob keys with bucket context.
- Archive activity logs use `action=archive_raw_source` with `resource_type=artifact`.
- Integration/E2E coverage:
  - `tests/integration/test_webapp_manifests_rerun_rewrites_to_blobs.py`
  - `tests/integration/test_tabular_asset_rerun_from_blobs_e2e.py`
- Integration/E2E coverage:
  - `tests/integration/test_webapp_manifests_rerun_rewrites_to_blobs.py`
  - `tests/integration/test_tabular_asset_rerun_from_blobs_e2e.py`

Details: `services/dagster/etl_pipelines/AGENTS.md`.
