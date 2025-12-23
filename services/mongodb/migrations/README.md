# MongoDB Schema Migrations

Versioned, idempotent schema migrations for the metadata ledger.

## Migration History

> Migrations consolidated on 2025-12-23 (Milestone 4).
> Original 001-004 squashed into single baseline.

## Current Migrations

| Version | File | Purpose |
|---------|------|---------|
| 001 | `001_baseline_schema.py` | Complete baseline (assets, manifests, runs, lineage) |
| 002 | `002_add_text_search.py` | Text index for keyword search |

## Schema Management Pattern

1. **Pydantic models** (`libs/models/`) are source of truth for Python
2. **Migrations** contain FROZEN schema constants (never modified after creation)
3. **Generator script** (`scripts/generate_migration_schema.py`) creates schemas from Pydantic
4. **Parity tests** (`tests/unit/test_schema_parity.py`) detect drift

### Workflow: Changing Schema

```
1. Update libs/models/*.py
2. Run parity test → FAILS (expected)
3. Generate schema: python scripts/generate_migration_schema.py Asset
4. Create new migration with frozen schema constant + data migration
5. Update parity test for new migration
6. Run migrations
```

## Indexes

### Assets
- `s3_key` (unique)
- `dataset_id+version` (compound)
- `content_hash`, `run_id`, `kind`, `created_at`
- `metadata.keywords` (multikey for array)
- `kind+dataset_id` (compound)
- `metadata.keywords` (text) - for full-text search

### Manifests
- `batch_id` (unique), `status`, `ingested_at`

### Runs
- `dagster_run_id` (unique, sparse), `batch_id`, `status`, `batch_id+started_at`

### Lineage
- `source_asset_id`, `target_asset_id`, `run_id`

## Developer Recovery

If schema validation errors block development:

```powershell
# Drop specific collections
python scripts/reset_mongodb.py --collections assets manifests --confirm

# Or drop all ETL collections
python scripts/reset_mongodb.py --all --confirm
```

See also: `docker compose down -v` for nuclear reset.
