# data-etl-dagster — Agent Guide

This repo implements an offline-first spatial data ETL platform orchestrated by Dagster.

## Start here
- `docs/agents/README.md` (progressive disclosure index)
- `services/dagster/etl_pipelines/AGENTS.md` (pipeline rules)
- `libs/models/AGENTS.md` (data model contracts)

## Global invariants
- Offline-first runtime (no public cloud dependencies).
- MongoDB is the ledger of record; no Mongo record = data does not exist.
- PostGIS is transient compute only (never persist durable datasets there).
- GDAL/heavy spatial libs are isolated to the user-code container.
- Ingestion contract: landing zone -> processing -> data lake (no direct writes to lake).
- Raw uploads are archived as artifacts that reference content-addressed blobs in `s3://data-lake/blobs/...`.
- Archive flow is hash-first, upload-second to avoid memory-heavy hashing when a blob already exists.
- Audit logging required for lifecycle/access events (`activity_logs`).
- Column schemas captured for tabular/spatial outputs; geometry type captured for spatial/joined.
- CSV headers are normalized to SQL-safe identifiers for joins.

## Data objects
- **Blobs**: content-addressed raw bytes stored under `s3://data-lake/blobs/...`.
- **Artifacts**: per-upload raw/intermediate references that point to blobs (includes source path + bucket).
- **Assets**: versioned, queryable outputs produced by the pipeline.

## Testing
See `docs/agents/testing.md`.
