# services/webapp — Agent Guide

## Scope
FastAPI tooling webapp for managing manifests and runs without direct access to
Dagster, MongoDB, or MinIO.

## Key invariants
- Manifest schema endpoint returns JSON Schema from `ManifestCreateRequest`.
- Schema must remain Ajv-compatible (draft-07) for client validation.
- For complex spreadsheet manifests, schema output must expose the versioned template union in an Ajv-compatible shape, and the UI must treat `template_id` as the discriminator.
- Webapp only talks to MinIO, MongoDB, and Dagster GraphQL.

## References
- Webapp README: `services/webapp/README.md`
- Model contracts: `libs/models/AGENTS.md`
- Testing: `docs/agents/testing.md`
