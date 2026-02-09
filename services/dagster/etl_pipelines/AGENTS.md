# services/dagster/etl_pipelines — Agent Guide

## Scope
Dagster code location: assets, ops, sensors, resources, and partitioning logic.

## Key invariants
- Register new assets/jobs/sensors/resources in `definitions.py`.
- Assets use dynamic partitions keyed by `dataset_id`.
- Spatial ops use PostGIS transient schemas; always clean up.
- `joined_spatial_asset` uses DuckDB (httpfs + spatial) and enforces GeoParquet metadata.
- Sensors are one-shot and archive processed manifests.
- Audit lifecycle events are recorded in `activity_logs`.
- Raw source archival writes `artifacts` that reference content-addressed `blobs` in `data-lake/blobs/`.
- Archive flow is hash-first, upload-second to avoid memory-heavy hashing when a blob already exists.
- Rerun flows rewrite manifest file paths to `s3://data-lake/blobs/...` using the latest raw_source artifact per source path.

## References
- Pipeline behavior: `docs/agents/pipelines.md`
- Data model contracts: `libs/models/AGENTS.md`
- Dagster orchestration: `services/dagster/AGENTS.md`

## Complex spreadsheet splitter

The `split_complex_spreadsheet_op` dispatches behavior by `template_id` from the manifest's `metadata.complex_spreadsheet` config. New templates are additive handlers — add a new branch for the new `template_id` without modifying existing template logic. All templates must remain backward compatible (see `libs/models/AGENTS.md` for the versioning contract).
