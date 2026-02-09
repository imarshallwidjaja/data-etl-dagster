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

## References
- Root guide: `AGENTS.md`
- MongoDB migrations: `services/mongodb/AGENTS.md`

## Complex spreadsheet templates

`manifests.metadata.complex_spreadsheet` represents per-manifest processing configuration — it is not a stored template library. The field carries a `template_id` discriminator and a typed `template_params` object that together tell the splitter op how to decompose a multi-table XLSX.

### Versioning contract

- `template_id` is a versioned discriminator (e.g. `anchor_unpivot_v1`). New behavior is additive: introduce a new ID like `anchor_unpivot_v2` rather than changing the meaning of an existing one.
- `template_params` schema is versioned in code via typed Pydantic models (`ComplexSpreadsheetTemplateParamsV1`, future `V2`, etc.).
- Once multiple template versions exist, model the config as a discriminated union on `template_id`.

### Backwards compatibility

- Never change the semantics of an existing `*_v1` template; add a new version instead.
- The current MongoDB migration (004) only allows the `complex_spreadsheet` field at the schema level. Adding new template versions typically does not require a Mongo migration unless DB-level enforcement of `template_params` shape is added.
