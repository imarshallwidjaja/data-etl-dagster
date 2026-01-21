# services/postgis — Agent Guide

## Scope
PostGIS initialization and transient compute for spatial workflows.

## Key invariants
- Never persist durable datasets in PostGIS.
- Use ephemeral schemas per run and always clean up.
- Geometry column name is standardized to `geom`.

## References
- Init SQL: `services/postgis/init/01-init-extensions.sql`
- Dagster pipelines: `services/dagster/etl_pipelines/AGENTS.md`
