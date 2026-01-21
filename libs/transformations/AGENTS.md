# libs/transformations — Agent Guide

## Scope
Recipe-based transformation system for manifest intents.

## Key invariants
- All transformations go through recipes (no inline transform logic in ops/assets).
- Steps are stateless; configuration via constructor args only.
- Deterministic: same inputs -> same outputs.
- Validate identifiers before use.

## References
- Root guide: `AGENTS.md`
- Dagster orchestration: `services/dagster/AGENTS.md`
