# services/dagster — Agent Guide

## Scope
This directory owns the Dagster orchestration layer: instance config, container
images, and the ETL code location wiring.

## Key invariants
- Register assets/jobs/sensors/resources in `etl_pipelines/definitions.py`.
- Heavy spatial deps (GDAL) live only in the user-code image.
- PostGIS schemas are ephemeral and must be cleaned up.
- Sensors are one-shot and archive manifests after processing.
- Run status sensors update Mongo `runs` and `manifests` on completion.
- Test runs should be tagged with `testing=true` (GraphQL executionMetadata tags or manifest metadata tags) so cleanup and filtering behave consistently.

## Working here
- After changing `libs/`, rebuild the user-code image: `docker compose build user-code`.
- Runtime config is in `docker-compose.yaml`; env var contracts live in `libs/models/AGENTS.md`.

## References
- Pipelines: `etl_pipelines/AGENTS.md`
- Pipeline details: `docs/agents/pipelines.md`
- Testing: `docs/agents/testing.md`
