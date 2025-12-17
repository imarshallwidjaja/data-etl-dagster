# MongoDB Schema Migrations

This directory contains **versioned, idempotent** MongoDB schema migrations for the platform’s **metadata ledger**.

The migration runner is `scripts/migrate_db.py`.

## Why migrations exist

MongoDB init scripts in `services/mongodb/init/` only run on **first** container startup (fresh volume).
Migrations let us evolve schema/validators/indexes safely on **existing** deployments without wiping data.

## How it works

- **Discovery**: the runner loads `NNN_description.py` files (3-digit prefix) from this directory.
- **Ordering**: migrations are applied in ascending `NNN` order.
- **Tracking**: applied versions are recorded in the `schema_migrations` collection.
- **Retry semantics**: if a migration fails, it is **not** recorded and will be retried on the next run.

## Migration file contract

Each migration file must define:

```python
from pymongo.database import Database

VERSION = "001"  # must match filename prefix

def up(db: Database) -> None:
    ...
```

## Adding a new migration

1. Create `services/mongodb/migrations/NNN_short_description.py`
2. Set `VERSION = "NNN"` to match the filename prefix exactly
3. Implement `up(db)` (make it idempotent: safe to run more than once)
4. Run locally: `python scripts/migrate_db.py`

## Notes

- `001_baseline_schema.py` includes tabular support; `002_add_tabular_contracts.py` is effectively a no-op for fresh installs.


