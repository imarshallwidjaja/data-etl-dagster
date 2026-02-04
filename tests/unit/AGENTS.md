# tests/unit — Agent Guide

## Scope
Fast unit tests with mocks/fakes; no Docker dependencies.

## Key invariants
- Mock external services (MinIO, MongoDB, PostGIS).
- Mock GDAL calls (`subprocess.run`) and boto3 in unit tests.
- Keep fixtures small and deterministic.

## References
- Test commands: `docs/agents/testing.md`
