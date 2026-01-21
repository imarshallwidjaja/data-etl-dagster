# libs/models — Agent Guide

## Scope
Pydantic v2 models that define manifests, assets, spatial types, and runtime settings.
These are the core contracts for ingestion and persistence.

## Key invariants
- Pydantic v2 only (`model_config`, `model_dump`, `@field_validator`).
- Models that map to MongoDB documents must match migration schemas.
- Unified status semantics for manifests/runs: `running`, `success`, `failure`, `canceled`.
- `HumanMetadataMixin` fields are required (`title`, `description`, `keywords`, `source`, `license`, `attribution`).
- Use `AssetMetadata.from_manifest_metadata()` for consistent metadata propagation.
- Manifest intent/type coherence is enforced (tabular intents require tabular files; join intents require `join_config`).
- Spatial/joined assets must include `metadata.geometry_type`; tabular/spatial/joined assets require `metadata.column_schema`.
- `Asset.run_id` and lineage records use Mongo ObjectId strings (not Dagster run IDs).
- JSON Schema from `ManifestCreateRequest` is a public API consumed by the webapp.

## Environment variables
- Canonical settings live in `libs/models/config.py`.
- `MINIO_ENDPOINT` must be `host:port` without scheme.

## References
- Root guide: `AGENTS.md`
- MongoDB migrations: `services/mongodb/AGENTS.md`
