# .github/workflows — Agent Guide

## Scope
GitHub Actions workflows and reusable CI jobs.

## Key invariants
- Reusable workflows are prefixed with `_` and called by `ci.yml`.
- E2E runs only after unit + integration succeed.
- Path filters must be updated when adding new test categories.

## References
- Root guide: `AGENTS.md`
- Scripts: `scripts/AGENTS.md`
- Tests: `tests/AGENTS.md`
