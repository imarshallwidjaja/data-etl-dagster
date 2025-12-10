# Service Context: PostGIS Compute Engine

## Overview

PostGIS serves as a **transient compute engine** for spatial operations. It is explicitly NOT used for data persistence. The pipeline follows a strict pattern: Load → Transform → Dump → Drop.

## Responsibilities

1. **Spatial SQL Execution:** Run PostGIS/PostgreSQL spatial functions.
2. **Ephemeral Processing:** Host temporary schemas for each pipeline run.
3. **Format Transformation:** Convert between spatial formats via SQL.
4. **Geometric Operations:** Buffer, intersect, union, clip, reproject, etc.

## Architecture Components

### Containers

| Container | Purpose | Port |
|-----------|---------|------|
| `postgis` | PostgreSQL + PostGIS extensions | 5432 |

### Schema Pattern

Each Dagster run creates and destroys its own schema:

```
spatial_compute (database)
├── public                     # Shared reference data (optional)
├── proc_{run_id_1}           # Temporary schema (active run)
│   ├── raw_input
│   ├── transformed
│   └── output
├── proc_{run_id_2}           # Another active run
└── ...
```

## Input/Output Contracts

### The Compute Engine Pattern

```
┌─────────────────────────────────────────────────────────────────┐
│                      Pipeline Execution                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   1. SPIN UP                                                     │
│      CREATE SCHEMA proc_{run_id};                               │
│                                                                  │
│   2. INGEST                                                      │
│      ogr2ogr -f PostgreSQL ... landing-zone/batch_XYZ/file.shp │
│      → proc_{run_id}.raw_input                                  │
│                                                                  │
│   3. PROCESS                                                     │
│      CREATE TABLE proc_{run_id}.transformed AS                  │
│      SELECT ST_Transform(geom, 4326), ...                       │
│      FROM proc_{run_id}.raw_input;                              │
│                                                                  │
│   4. EJECT                                                       │
│      ogr2ogr -f Parquet data-lake/output.parquet                │
│      PG:"..." proc_{run_id}.transformed                         │
│                                                                  │
│   5. TEARDOWN                                                    │
│      DROP SCHEMA proc_{run_id} CASCADE;                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Critical Rule: Always Teardown

```python
# Python pattern for guaranteed cleanup
try:
    # Create schema
    cursor.execute(f"CREATE SCHEMA proc_{run_id}")
    
    # ... processing ...
    
finally:
    # ALWAYS drop schema, even on failure
    cursor.execute(f"DROP SCHEMA IF EXISTS proc_{run_id} CASCADE")
```

## Relation to Global Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                  PostGIS (This Service)                       │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│   MinIO ──► ogr2ogr ──► PostGIS ──► ogr2ogr ──► MinIO        │
│   (landing)            (compute)              (lake)          │
│                                                               │
│   ┌─────────────────────────────────────────────────┐        │
│   │  PostGIS is a PROCESSING NODE, not STORAGE      │        │
│   │  Data enters, transforms, exits. Nothing stays. │        │
│   └─────────────────────────────────────────────────┘        │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

## Configuration Requirements

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `POSTGRES_USER` | Database username | postgis |
| `POSTGRES_PASSWORD` | Database password | postgis_password |
| `POSTGRES_DB` | Database name | spatial_compute |

### PostGIS Extensions

The following extensions are pre-installed:

- `postgis` - Core spatial functions
- `postgis_topology` - Topology support
- `postgis_raster` - Raster support
- `fuzzystrmatch` - String matching
- `postgis_tiger_geocoder` - Geocoding (optional)

## Development Notes

- Connect via:
  ```bash
  psql -h localhost -p 5432 -U postgis -d spatial_compute
  ```
- Monitor active schemas:
  ```sql
  SELECT schema_name FROM information_schema.schemata 
  WHERE schema_name LIKE 'proc_%';
  ```
- Emergency cleanup (development only):
  ```sql
  DO $$ 
  DECLARE r RECORD;
  BEGIN
    FOR r IN SELECT schema_name FROM information_schema.schemata 
             WHERE schema_name LIKE 'proc_%'
    LOOP
      EXECUTE 'DROP SCHEMA ' || r.schema_name || ' CASCADE';
    END LOOP;
  END $$;
  ```

## Volume Note

The PostGIS volume exists primarily for development convenience. In production, this database should be considered fully ephemeral - losing the volume should have zero impact on the system's data integrity (all permanent data lives in MinIO/MongoDB).

