# Platform Invariants (Summary)

These are cross-cutting rules that apply across the codebase.

## Core invariants
- Offline-first runtime (no public cloud dependencies).
- MongoDB is the ledger of record: no Mongo record = data does not exist.
- PostGIS is transient compute only (never persist durable datasets there).
- GDAL/heavy spatial libs are isolated to the user-code container.
- Ingestion contract: landing zone -> processing -> data lake (no direct writes to lake).
- Audit logging required for lifecycle and access events (`activity_logs`).
- Column schema captured for tabular + spatial outputs; types normalized.
- Geometry type captured for spatial/joined outputs.
- CSV headers cleaned to valid SQL identifiers for joins.

## Canonical contracts
- Metadata and model enforcement: `libs/models/AGENTS.md`
- Ledger schema/migrations: `services/mongodb/AGENTS.md`
- Dagster pipeline routing: `docs/agents/pipelines.md`
