# services/mongodb/ — Agent Guide

## What this directory is / owns

This directory owns MongoDB initialization for the platform’s **metadata ledger**.
MongoDB is the Source of Truth: if it’s not recorded in Mongo, it doesn’t exist in the platform.

## Key invariants / non-negotiables

- **Ledger law**: MongoDB is canonical for manifests/assets/lineage.
- **Schema validation lives in init scripts**: keep them aligned with `libs/models/`.
- **Manifest metadata is disciplined**: only `project`, `description`, `tags` (primitive scalars), and `join_config` (structured join fields) are allowed; everything else is rejected.

## Entry points / key files

- `init/`: MongoDB init scripts (collections, schema validation, indexes)
- Dev configuration: `../../docker-compose.yaml`

## How to work here

- Docker init uses:
  - `MONGO_INITDB_ROOT_USERNAME`
  - `MONGO_INITDB_ROOT_PASSWORD`
  - `MONGO_INITDB_DATABASE` (wired from `MONGO_DATABASE` in compose)

- App/runtime settings (user-code) additionally use:
  - `MONGO_HOST`, `MONGO_PORT`, `MONGO_DATABASE`, `MONGO_AUTH_SOURCE`
  - `MONGO_CONNECTION_STRING` (preferred single setting; compose builds it for the `user-code` container)

## Common tasks

- **Change the ledger schema**:
  - update `init/` scripts
  - update Pydantic models in `libs/models/`
  - update unit tests that exercise Mongo behavior


## Schema Migrations

The migration system (`scripts/migrate_db.py`) replaces the legacy `init/01-init-db.js` for schema evolution.

### Key Behavior
- Migrations run automatically on container startup via `mongo-migration` service
- Applied migrations are tracked in `schema_migrations` collection
- Failed migrations are NOT recorded (will retry on next startup)
- Migration `001_baseline_schema.py` includes tabular support, making `002_add_tabular_contracts.py` a no-op for fresh installs

### Adding New Migrations
1. Create `services/mongodb/migrations/NNN_description.py`
2. Define `VERSION = "NNN"` and `def up(db): ...`
3. Test locally: `python scripts/migrate_db.py`

## Testing / verification

- Unit tests (mocked): `pytest tests/unit/test_mongodb_resource.py`
- Integration tests (Docker required): `pytest -m integration tests/integration/test_mongodb.py`

## Links

- Root guide: `../../AGENTS.md`
- Models/schemas: `../../libs/models/AGENTS.md`
- Dagster orchestration: `../dagster/AGENTS.md`
