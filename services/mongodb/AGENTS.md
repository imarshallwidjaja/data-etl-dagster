# services/mongodb — Agent Guide

## Scope
MongoDB initialization, migrations, and the platform metadata ledger.

## Key invariants
- MongoDB is the ledger of record for manifests, runs, assets, and lineage.
- Migration schemas are frozen constants; never edit existing migrations.
- Generate new schema constants via `scripts/generate_migration_schema.py`.
- Human metadata fields and join rules must match `libs/models` contracts.

## References
- Migrations: `services/mongodb/migrations/README.md`
- Model contracts: `libs/models/AGENTS.md`
- Env vars/config: `docker-compose.yaml`
