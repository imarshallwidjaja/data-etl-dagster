# tests — Agent Guide

## Scope
Project test suite (unit + integration).

## Key invariants
- Unit tests are fast and mocked (no Docker).
- Integration tests require Docker services and must carry markers.
- E2E tests are `@pytest.mark.e2e` under the integration suite.

## References
- Test commands: `docs/agents/testing.md`
- Unit tests: `tests/unit/AGENTS.md`
- Integration tests: `tests/integration/AGENTS.md`
