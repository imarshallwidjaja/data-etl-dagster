# services/mongodb/ — Agent Guide

## What this directory is / owns

This directory owns MongoDB initialization for the platform's **metadata ledger**.
MongoDB is the Source of Truth: if it's not recorded in Mongo, it doesn't exist in the platform.

## Key invariants / non-negotiables

- **Ledger law**: MongoDB is canonical for manifests/assets/lineage.
- **Schema validation lives in migrations**: keep them aligned with `libs/models/`.
- **Manifest metadata is disciplined**: only `project`, `description`, `tags` (primitive scalars), and `join_config` (structured join fields) are allowed; everything else is rejected.

## Entry points / key files

### Initialization

- `init/01-init-db.js`: Legacy MongoDB init script (collections, schema validation, indexes)
- Dev configuration: `../../docker-compose.yaml`

### Migrations

- `migrations/001_baseline_schema.py`: Baseline schema (collections, indexes, validation for spatial/tabular/joined)
- `migrations/002_add_tabular_contracts.py`: Tabular contracts migration (no-op for fresh installs)
- `migrations/003_add_runs_collection.py`: Adds runs collection, unifies status values (running/success/failure/canceled), updates asset/lineage to use run_id ObjectId
- `migrations/README.md`: Migration system documentation

## How to work here

### Environment variables

- **Docker init** uses:
  - `MONGO_INITDB_ROOT_USERNAME`
  - `MONGO_INITDB_ROOT_PASSWORD`
  - `MONGO_INITDB_DATABASE` (wired from `MONGO_DATABASE` in compose)

- **App/runtime settings** (user-code) additionally use:
  - `MONGO_HOST`, `MONGO_PORT`, `MONGO_DATABASE`, `MONGO_AUTH_SOURCE`
  - `MONGO_CONNECTION_STRING` (preferred single setting; compose builds it)

### Collections

| Collection | Purpose |
|------------|---------|
| `manifests` | Ingestion manifest records (status: running/success/failure/canceled) |
| `runs` | Dagster run tracking (links manifests to assets) |
| `assets` | Asset registry (spatial, tabular, joined) - uses `run_id` ObjectId |
| `lineage` | Parent→child asset relationships - uses `run_id` ObjectId |
| `schema_migrations` | Applied migration tracking |

### Status Values (Unified)

Manifests and runs use the same status enum:
- `running` - Processing in progress
- `success` - Completed successfully
- `failure` - Failed with error
- `canceled` - Canceled by user or system

## Schema Migrations

The migration system (`scripts/migrate_db.py`) handles schema evolution.

### Key Behavior

- Migrations run automatically on container startup via `mongo-migration` service
- Applied migrations are tracked in `schema_migrations` collection
- Failed migrations are NOT recorded (will retry on next startup)
- Migration `001_baseline_schema.py` includes tabular support, making `002_add_tabular_contracts.py` a no-op for fresh installs

### Adding New Migrations

1. Create `services/mongodb/migrations/NNN_description.py`
2. Define `VERSION = "NNN"` and `def up(db): ...`
3. Test locally: `python scripts/migrate_db.py`

See `migrations/README.md` for detailed migration authoring guidelines.

## Common tasks

- **Change the ledger schema**:
  1. Create a new migration in `migrations/`
  2. Update Pydantic models in `libs/models/`
  3. Update unit tests that exercise Mongo behavior
  4. Restart containers to apply migration

- **Debug migration issues**:
  ```python
  # Check applied migrations
  db.schema_migrations.find().sort("applied_at", -1)
  ```

## Testing / verification

- Unit tests (mocked): `pytest tests/unit/test_mongodb_resource.py`
- Integration (connectivity): `pytest -m integration tests/integration/test_mongodb.py`
- Integration (init): `pytest -m integration tests/integration/test_mongodb_init.py`
- Integration (migrations): `pytest -m integration tests/integration/test_mongodb_migrations.py`

## Links

- Root guide: `../../AGENTS.md`
- Models/schemas: `../../libs/models/AGENTS.md`
- Dagster orchestration: `../dagster/AGENTS.md`
- Migration script: `../../scripts/migrate_db.py`
