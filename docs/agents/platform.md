# Platform Invariants (Summary)

These are cross-cutting rules that apply across the codebase.

## Core invariants
- Offline-first runtime (no public cloud dependencies).
- MongoDB is the ledger of record: no Mongo record = data does not exist.
- PostGIS is transient compute only (never persist durable datasets there).
- GDAL/heavy spatial libs are isolated to the user-code container.
- Ingestion contract: landing zone -> processing -> data lake (no direct writes to lake).
- Raw uploads are archived as artifacts that reference content-addressed blobs in `s3://data-lake/blobs/...`.
- Archive flow is hash-first, upload-second to avoid memory-heavy hashing when a blob already exists.
- Audit logging required for lifecycle and access events (`activity_logs`).
- Column schema captured for tabular + spatial outputs; types normalized.
- Geometry type captured for spatial/joined outputs.
- CSV headers cleaned to valid SQL identifiers for joins.

## Data objects
- **Blobs**: content-addressed raw bytes stored under `s3://data-lake/blobs/...`.
- **Artifacts**: per-upload raw/intermediate references that point to blobs (includes source path + bucket).
- **Assets**: versioned, queryable outputs produced by the pipeline.

## Canonical contracts
- Metadata and model enforcement: `libs/models/AGENTS.md`
- Ledger schema/migrations: `services/mongodb/AGENTS.md`
- Dagster pipeline routing: `docs/agents/pipelines.md`
