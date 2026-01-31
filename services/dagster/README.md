# Dagster Service

The Dagster orchestration layer for the Spatial Data ETL platform. Implements the **Load → Transform → Export** flow for spatial and tabular data ingestion.

## Quick Start

```bash
# Start all services
docker compose up -d

# Check service health
python scripts/wait_for_services.py

# Access Dagster UI
open http://localhost:3000
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Dagster                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │   Webserver  │  │    Daemon    │  │      User Code         │ │
│  │   (UI/API)   │  │  (Scheduler) │  │  (Python + GDAL libs)  │ │
│  └──────────────┘  └──────────────┘  └────────────────────────┘ │
│                                              │                   │
│  Sensors detect manifests → Jobs execute → Assets materialized  │
└─────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
    ┌─────────┐          ┌─────────┐          ┌─────────┐
    │  MinIO  │          │ PostGIS │          │ MongoDB │
    │ (files) │          │(compute)│          │(ledger) │
    └─────────┘          └─────────┘          └─────────┘
```

## Directory Structure

```
services/dagster/
├── etl_pipelines/           # Dagster code location
│   ├── definitions.py       # ★ Registration point for all Dagster objects
│   ├── partitions.py        # Dynamic partitions (dataset_id)
│   ├── assets/              # Software-defined assets
│   │   ├── base_assets.py   # raw_spatial_asset, raw_tabular_asset
│   │   ├── joined_assets.py # joined_spatial_asset
│   │   └── health_checks.py # Health check assets
│   ├── ops/                 # Operations (legacy + shared)
│   │   ├── load_op.py       # Load from MinIO
│   │   ├── transform_op.py  # PostGIS transformations
│   │   ├── export_op.py     # Export to data lake
│   │   ├── cleanup_op.py    # Schema cleanup
│   │   ├── tabular_ops.py   # CSV processing
│   │   └── join_ops.py      # Join operations
│   ├── sensors/             # Manifest detection
│   │   ├── manifest_sensor.py  # Legacy (ingest_job)
│   │   ├── spatial_sensor.py   # → spatial_asset_job
│   │   ├── tabular_sensor.py   # → tabular_asset_job
│   │   ├── join_sensor.py      # → join_asset_job
│   │   └── run_status_sensor.py # Run lifecycle → MongoDB updates
│   ├── resources/           # External service wrappers
│   │   ├── minio_resource.py
│   │   ├── mongodb_resource.py
│   │   ├── postgis_resource.py
│   │   └── gdal_resource.py
│   └── jobs/                # Job definitions
│       └── ingest_job.py    # Legacy op-based job
├── dagster.yaml             # Instance configuration
├── workspace.yaml           # Code location wiring
├── Dockerfile               # Webserver/Daemon image
├── Dockerfile.user-code     # User code image (includes GDAL)
├── requirements.txt         # Webserver/Daemon deps
└── requirements-user-code.txt  # User code deps
```

## Pipelines

### Intent → Pipeline Routing

| Manifest Intent | Sensor | Job | Asset |
|-----------------|--------|-----|-------|
| `ingest_vector` | `spatial_sensor` | `spatial_asset_job` | `raw_spatial_asset` |
| `ingest_raster` | `spatial_sensor` | `spatial_asset_job` | `raw_spatial_asset` |
| `ingest_tabular` | `tabular_sensor` | `tabular_asset_job` | `raw_tabular_asset` |
| `join_datasets` | `join_sensor` | `join_asset_job` | `joined_spatial_asset` |
| Other spatial | `manifest_sensor` | `ingest_job` | (legacy op-based) |

### Pipeline Flows

**Spatial Ingestion:**
```
Manifest → Sensor → Job
  └→ Load (MinIO → PostGIS)
  └→ Transform (PostGIS SQL)
  └→ Export (PostGIS → GeoParquet → MinIO)
  └→ Register (MongoDB)
  └→ Cleanup (Drop PostGIS schema)
```

**Tabular Ingestion:**
```
Manifest → Sensor → Job
  └→ Load (MinIO → Memory)
  └→ Process (Pandas/PyArrow)
  └→ Export (Parquet → MinIO)
  └→ Register (MongoDB)
```

**Join Workflow:**
```
Manifest → Sensor → Job
  └→ Resolve Assets (MongoDB lookup)
  └→ Load Both (MinIO → PostGIS)
  └→ SQL Join (PostGIS)
  └→ Export (GeoParquet → MinIO)
  └→ Register + Lineage (MongoDB)
  └→ Cleanup (Drop PostGIS schema)
```

---

## Development

### Adding a New Asset

1. **Create the asset** in `etl_pipelines/assets/`:
   ```python
   from dagster import asset, AssetExecutionContext
   from ..partitions import dataset_partitions

   @asset(
       partitions_def=dataset_partitions,
       required_resource_keys={"minio", "mongodb", "postgis"},
   )
   def my_new_asset(context: AssetExecutionContext):
       ...
   ```

2. **Register in `definitions.py`**:
   ```python
   from .assets import my_new_asset
   
   defs = Definitions(
       assets=[..., my_new_asset],
       ...
   )
   ```

3. **Add tests** in `tests/unit/` and `tests/integration/`.

### Adding a New Sensor

1. **Create the sensor** in `etl_pipelines/sensors/`:
   ```python
   from dagster import sensor, RunRequest, SensorEvaluationContext

   @sensor(
       job=my_asset_job,
       required_resource_keys={"minio"},
   )
   def my_sensor(context: SensorEvaluationContext):
       ...
       yield RunRequest(...)
   ```

2. **Register in `definitions.py`**.

### Rebuilding After Changes

```bash
# After changing etl_pipelines/
docker compose restart user-code

# After changing libs/
docker compose build user-code && docker compose up -d user-code
```

---

## Configuration

### Environment Variables

| Variable | Container | Description |
|----------|-----------|-------------|
| `DAGSTER_POSTGRES_HOST` | all | Dagster metadata DB host |
| `DAGSTER_POSTGRES_USER` | all | Dagster metadata DB user |
| `DAGSTER_POSTGRES_PASSWORD` | all | Dagster metadata DB password |
| `DAGSTER_POSTGRES_DB` | all | Dagster metadata DB name |
| `MINIO_ENDPOINT` | user-code | MinIO endpoint (host:port) |
| `MONGO_CONNECTION_STRING` | user-code | MongoDB connection string |
| `POSTGRES_HOST` | user-code | PostGIS host (compute) |
| `MANIFEST_ROUTER_ENABLED_LANES` | user-code | Legacy sensor lane control |

### Docker Images

| Image | Purpose | Key Dependencies |
|-------|---------|------------------|
| `dagster-webserver` | UI + GraphQL API | dagster, dagster-webserver |
| `dagster-daemon` | Sensor/Schedule runner | dagster |
| `user-code` | Pipeline execution | dagster, GDAL, geopandas, pyarrow |

---

## Testing

```bash
# Unit tests (no Docker required)
pytest tests/unit -v

# Integration tests (Docker required)
docker compose up -d
python scripts/wait_for_services.py
pytest -m "integration and not e2e" tests/integration -v

# E2E tests (full pipeline via GraphQL)
pytest -m "integration and e2e" tests/integration -v
```

Test runs should be tagged with `testing=true` so cleanup can filter correctly. For GraphQL launches, include `executionMetadata.tags` (use `build_test_run_tags` from `tests/integration/helpers.py`). For manifest-driven runs, set `metadata.tags.testing=true` in the manifest.

---

## Troubleshooting

### Sensor not picking up manifests?

```bash
# Check sensor logs
docker logs user-code 2>&1 | grep -i sensor

# Verify manifest exists in MinIO
mc ls local/landing-zone/manifests/
```

### Job failed?

1. Check Dagster UI at `http://localhost:3000`
2. Navigate to Runs → Find failed run → View logs
3. Check for PostGIS schema cleanup issues

### User code not loading?

```bash
# Check user-code container health
docker logs user-code

# Verify code location is registered
curl http://localhost:3000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"{ repositoryLocationsOrError { ... on RepositoryLocationConnection { nodes { name } } } }"}'
```

### Need to manually trigger a job?

```python
# Via GraphQL (see tests/integration/test_*_e2e.py for examples)
mutation {
  launchRun(executionParams: {
    selector: {
      repositoryLocationName: "etl_pipelines"
      repositoryName: "__repository__"
      pipelineName: "spatial_asset_job"
    }
    runConfigData: {...}
    executionMetadata: {
      tags: [{key: "dagster/partition", value: "my_dataset"}]
    }
  }) { ... }
}
```

---

## Related Documentation

- [AGENTS.md](./AGENTS.md) - AI agent context (detailed sensor/cursor behavior)
- [Root AGENTS.md](../../AGENTS.md) - Platform overview and manifest protocol
- [libs/models](../../libs/models/AGENTS.md) - Pydantic model definitions
