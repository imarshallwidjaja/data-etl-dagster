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

## Sensor routing
- `spatial_sensor` -> `spatial_asset_job` (spatial intents)
- `tabular_sensor` -> `tabular_asset_job` (tabular intents)
- `join_sensor` -> `join_asset_job` (join intent)
- `manifest_sensor` handles legacy op-based ingestion

## Run tracking
- Run lifecycle lives in MongoDB `runs` collection.
- Assets and lineage reference Mongo `run_id` (ObjectId string).

Details: `services/dagster/etl_pipelines/AGENTS.md`.
