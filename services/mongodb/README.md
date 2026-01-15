# MongoDB Service

MongoDB serves as the **metadata ledger** for the Spatial Data ETL platform. If it's not recorded in MongoDB, it doesn't exist.

## Quick Start

```bash
# Start the services (includes MongoDB and migration runner)
docker compose up -d mongodb mongo-migration

# Check migration status
docker logs mongo-migration

# Run migrations manually (from repo root)
python scripts/migrate_db.py
```

## Architecture Role

MongoDB stores:

| Collection | Purpose |
|------------|---------|
| `manifests` | Ingestion manifest records (batch_id, intent, files, metadata, status) |
| `assets` | Asset registry with versioning (s3_key, dataset_id, version, kind) |
| `runs` | Dagster run tracking (dagster_run_id, batch_id, status, asset_ids) |
| `lineage` | Parent → child asset relationships (for join provenance) |
| `activity_logs` | Audit trail of all platform operations with user and IP tracking |
| `schema_migrations` | Applied migration tracking |

## Directory Structure

```
services/mongodb/
├── init/
│   └── 01-init-db.js         # Legacy init script (runs on fresh volumes only)
├── migrations/
│   ├── 001_baseline_schema.py    # Baseline collections, indexes, validators
│   ├── 002_add_text_search.py     # Text index for keyword search
│   ├── 003_activity_logs.py       # Activity logging collection and indexes
│   └── README.md             # Migration authoring guide
└── AGENTS.md                 # AI agent context
```

---

## Schema Migrations

### Why Migrations?

The `init/` scripts only run on **first container startup** (fresh volumes). Migrations let you evolve schema on **existing deployments** without wiping data.

### How It Works

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ Container Start │────▶│ mongo-migration  │────▶│ MongoDB ready   │
│                 │     │ service runs     │     │ with schema     │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                               │
                               ▼
                        ┌──────────────────┐
                        │ scripts/         │
                        │ migrate_db.py    │
                        └──────────────────┘
                               │
                        Discovers migrations/NNN_*.py
                        Checks schema_migrations collection
                        Applies pending migrations in order
```

1. **Discovery**: Runner finds `NNN_description.py` files (3-digit prefix)
2. **Ordering**: Migrations apply in ascending `NNN` order
3. **Tracking**: Applied versions recorded in `schema_migrations` collection
4. **Retry**: Failed migrations are NOT recorded (will retry on next run)

### Creating a New Migration

**Step 1: Create the file**

```bash
# Use next available version number
touch services/mongodb/migrations/003_add_something.py
```

**Step 2: Write the migration**

```python
"""
003 - Add something new

Describe what this migration does.
"""
from pymongo.database import Database

VERSION = "003"  # MUST match filename prefix


def up(db: Database) -> None:
    """Apply migration. MUST be idempotent (safe to run multiple times)."""
    
    # Example: Add index (idempotent - create_index is safe to repeat)
    db["assets"].create_index("new_field", sparse=True)
    
    # Example: Add collection (idempotent - check first)
    if "new_collection" not in db.list_collection_names():
        db.create_collection("new_collection")
    
    # Example: Update documents (idempotent - use $setOnInsert or check)
    db["assets"].update_many(
        {"new_field": {"$exists": False}},
        {"$set": {"new_field": "default_value"}}
    )
```

**Step 3: Test locally**

```bash
# Ensure Docker stack is running
docker compose up -d mongodb

# Run migrations
python scripts/migrate_db.py

# Verify in MongoDB
docker exec -it mongodb mongosh -u admin -p admin --authenticationDatabase admin spatial_etl
> db.schema_migrations.find().sort({applied_at: -1})
```

**Step 4: Commit and deploy**

```bash
git add services/mongodb/migrations/003_add_something.py
git commit -m "migration: add something new"
```

### Migration Best Practices

| ✅ Do | ❌ Don't |
|-------|---------|
| Make migrations idempotent | Assume first-time run |
| Use `create_index` (safe to repeat) | Use raw `createIndex` commands |
| Check collection exists before create | Drop and recreate collections |
| Use `update_many` with `$exists` checks | Overwrite existing data blindly |
| Include rollback notes in comments | Leave migrations undocumented |

### Checking Migration Status

```javascript
// In mongosh
db.schema_migrations.find().sort({applied_at: -1})

// Example output:
{
  version: "002",
  applied_at: ISODate("2024-01-15T10:30:00Z"),
  duration_ms: 245
}
```

### Troubleshooting

**Migration stuck/failed?**

```bash
# Check logs
docker logs mongo-migration

# If you need to re-run a migration (dangerous!):
docker exec -it mongodb mongosh -u admin -p admin --authenticationDatabase admin spatial_etl
> db.schema_migrations.deleteOne({version: "003"})

# Then restart mongo-migration
docker compose restart mongo-migration
```

**Need to debug locally?**

```bash
# Set env vars (or use .env file)
export MONGO_HOST=localhost
export MONGO_PORT=27017
export MONGO_INITDB_ROOT_USERNAME=admin
export MONGO_INITDB_ROOT_PASSWORD=admin
export MONGO_DATABASE=spatial_etl

python scripts/migrate_db.py
```

---

## Environment Variables

| Variable | Used By | Default | Description |
|----------|---------|---------|-------------|
| `MONGO_HOST` | user-code | `mongodb` | MongoDB hostname |
| `MONGO_PORT` | user-code | `27017` | MongoDB port |
| `MONGO_DATABASE` | all | `spatial_etl` | Database name |
| `MONGO_INITDB_ROOT_USERNAME` | Docker | (required) | Admin username |
| `MONGO_INITDB_ROOT_PASSWORD` | Docker | (required) | Admin password |
| `MONGO_AUTH_SOURCE` | user-code | `admin` | Auth database |
| `MONGO_CONNECTION_STRING` | user-code | (computed) | Full connection string |

---

## Development

### Connecting Manually

```bash
# Via Docker
docker exec -it mongodb mongosh -u admin -p admin --authenticationDatabase admin spatial_etl

# Via local mongosh (if installed)
mongosh "mongodb://admin:admin@localhost:27017/spatial_etl?authSource=admin"
```

### Useful Queries

```javascript
// List all assets
db.assets.find().limit(5).pretty()

// Find asset by dataset_id
db.assets.find({dataset_id: "my_dataset"}).sort({version: -1})

// Check lineage for an asset
db.lineage.find({target_asset_id: ObjectId("...")})

// View recent manifests
db.manifests.find().sort({created_at: -1}).limit(10)
```

---

## Related Documentation

- [AGENTS.md](./AGENTS.md) - AI agent context for this directory
- [migrations/README.md](./migrations/README.md) - Detailed migration authoring guide
- [libs/models/AGENTS.md](../../libs/models/AGENTS.md) - Pydantic model definitions
