# tests/integration — Agent Guide

## Scope
Integration and E2E tests against live Docker services.

## Key invariants
- Mark integration tests with `@pytest.mark.integration`.
- Mark E2E tests with `@pytest.mark.e2e`.
- Tests require healthy Docker services (MinIO, MongoDB, PostGIS, Dagster).
- Tests must clean up artifacts they create (manifests, runs, assets, partitions).
- Cleanup must happen inside the test/fixture (use `try/finally`); pre-clean fixtures do not run post-test cleanup.
- Tag Dagster runs with `testing=true` in `executionMetadata.tags` (GraphQL) or `metadata.tags.testing=true` in manifests (sensor-triggered).

## References
- Test commands: `docs/agents/testing.md`
- Fixtures: `tests/integration/fixtures/AGENTS.md`
