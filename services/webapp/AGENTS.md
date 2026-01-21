# services/webapp — Agent Guide

## Scope
FastAPI tooling webapp for managing manifests and runs without direct access to
Dagster, MongoDB, or MinIO.

## Key invariants
- Manifest schema endpoint returns JSON Schema from `ManifestCreateRequest`.
- Schema must remain Ajv-compatible (draft-07) for client validation.
- Webapp only talks to MinIO, MongoDB, and Dagster GraphQL.

## References
- Webapp README: `services/webapp/README.md`
- Model contracts: `libs/models/AGENTS.md`
- Testing: `docs/agents/testing.md`
