# services/mongodb — Agent Guide

## Scope
MongoDB initialization, migrations, and the platform metadata ledger.

## Key invariants
- MongoDB is the ledger of record for manifests, runs, assets, and lineage.
- Migration schemas are frozen constants; never edit existing migrations (see pre-deploy exception below).
- Generate new schema constants via `scripts/generate_migration_schema.py`.
- Human metadata fields and join rules must match `libs/models` contracts.

## Pre-deploy exception (baseline migrations)
The blob/artifact rollout updated baseline migration files in-place to introduce `blobs`, `artifacts`, and
the `archive_raw_source` activity enum. This was a one-off exception because no production deployment
existed yet. Post-deploy, the frozen migration rule applies again.

## Ledger additions
- `blobs` store content-addressed raw bytes (data lake `blobs/` prefix).
- `artifacts` store per-upload raw/intermediate references to blobs.
- `activity_logs` use `action=archive_raw_source` with `resource_type=artifact` for raw archival.

## References
- Migrations: `services/mongodb/migrations/README.md`
- Model contracts: `libs/models/AGENTS.md`
- Env vars/config: `docker-compose.yaml`
