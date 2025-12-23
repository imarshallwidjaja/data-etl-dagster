# services/mongodb/ — Agent Guide

## What this directory is / owns

This directory owns MongoDB initialization for the platform's **metadata ledger**.
MongoDB is the Source of Truth: if it's not recorded in Mongo, it doesn't exist in the platform.

## Key invariants / non-negotiables

- **Ledger law**: MongoDB is canonical for manifests/assets/lineage.
- **Schema validation lives in migrations**: keep them aligned with `libs/models/`.
- **Schema management**: Migrations contain frozen schema constants. Use `scripts/generate_migration_schema.py` to generate from Pydantic, then paste as immutable constant. Never modify existing migration schemas.
- **Manifest metadata is disciplined**: Human metadata fields (title, description, keywords, source, license, attribution) are required; `project` is optional; `tags` allows only primitive scalars.

## Entry points / key files

### Initialization

- `init/01-init-db.js`: Legacy MongoDB init script (collections, schema validation, indexes)
- Dev configuration: `../../docker-compose.yaml`

### Migrations

- `migrations/001_baseline_schema.py`: Squashed baseline (assets, manifests, runs, lineage collections with frozen schemas)
- `migrations/002_add_text_search.py`: Text index for keyword search
- `migrations/README.md`: Migration system and schema management documentation

> **Note**: Migration files use `importlib` in parity tests since filenames start with digits.

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
- Parity tests (`tests/unit/test_schema_parity.py`) detect drift between Pydantic and MongoDB schemas

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
