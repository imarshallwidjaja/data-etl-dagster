# tests/integration — Agent Guide

## Scope
Integration and E2E tests against live Docker services.

## Key invariants
- Mark integration tests with `@pytest.mark.integration`.
- Mark E2E tests with `@pytest.mark.e2e`.
- Tests require healthy Docker services (MinIO, MongoDB, PostGIS, Dagster).
- Tests must clean up artifacts they create (manifests, runs, assets, partitions).

## References
- Test commands: `docs/agents/testing.md`
- Fixtures: `tests/integration/fixtures/AGENTS.md`
