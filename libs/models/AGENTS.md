# libs/models — Agent Guide

## Scope
Pydantic v2 models that define manifests, assets, spatial types, and runtime settings.
These are the core contracts for ingestion and persistence.

## Key invariants
- Pydantic v2 only (`model_config`, `model_dump`, `@field_validator`).
- Use strict typing; prefer `typing.Annotated` for structured metadata and validators.
- Models that map to MongoDB documents must match migration schemas.
- Unified status semantics for manifests/runs: `running`, `success`, `failure`, `canceled`.
- `HumanMetadataMixin` fields are required (`title`, `description`, `keywords`, `source`, `license`, `attribution`).
- Use `AssetMetadata.from_manifest_metadata()` for consistent metadata propagation.
- Manifest intent/type coherence is enforced (tabular intents require tabular files; join intents require `join_config`).
- Spatial/joined assets must include `metadata.geometry_type`; tabular/spatial/joined assets require `metadata.column_schema`.
- `Asset.run_id` and lineage records use Mongo ObjectId strings (not Dagster run IDs).
- JSON Schema from `ManifestCreateRequest` is a public API consumed by the webapp.
- Raw source archival uses `Blob` (content-addressed bytes) and `Artifact` (per-upload raw/intermediate) models.
- Activity logs include `action=archive_raw_source` with `resource_type=artifact` for raw archival.

## Environment variables
- Canonical settings live in `libs/models/config.py`.
- `MINIO_ENDPOINT` must be `host:port` without scheme.

## Complex spreadsheet templates
- `manifests.metadata.complex_spreadsheet` is per-manifest processing config, not a stored template library.
- `template_id` is a versioned discriminator. New behavior is additive via new IDs (for example `anchor_unpivot_v2`).
- `template_params` schema is versioned in code via typed models (for example `ComplexSpreadsheetTemplateParamsV1` and future `...V2`).
- Backwards compatibility rule: never change the meaning of an existing `*_v1` template; introduce a new `*_v2` template ID instead.
- Implementation rule: once multiple versions exist, model complex spreadsheet config as a discriminated union on `template_id`.
- Mongo migration rule: the current migration only allows the `complex_spreadsheet` field. Adding new template versions typically does not require a Mongo migration unless DB-level `template_params` shape enforcement is introduced.

## References
- Root guide: `AGENTS.md`
- MongoDB migrations: `services/mongodb/AGENTS.md`
