# libs/transformations/ — Agent Guide

## What this directory is / owns

This library implements the **recipe-based transformation architecture** used by the Dagster pipeline.
It maps a manifest `intent` to a list of PostGIS SQL steps, executed sequentially with table chaining.

## Key invariants / non-negotiables

- **PostGIS is transient compute**: SQL should assume ephemeral schemas per run.
- **Deterministic SQL**: same inputs → same SQL.
- **Geometry column contract**: vector transforms operate on a single geometry column (default: `geom`) and must not proliferate geometry columns.
- **Identifier safety**: schema/table/column identifiers must be validated before SQL generation.

## Entry points / key files

- `registry.py`: `RecipeRegistry` (intent → list[steps])
- `vector.py`: vector steps (CRS normalization, simplification, spatial index)
- `base.py`: base step interfaces

## How to work here

- **Add a new step**:
  - implement a new `VectorStep`/`TransformStep`
  - keep it stateless (configuration via constructor args)
  - add unit tests for SQL generation + edge cases

- **Add/modify a recipe**:
  - update `RecipeRegistry` to map an `intent` string to the desired step list
  - add tests asserting recipe selection and ordering

## Common tasks

- **Tune simplification tolerance**: update the relevant step defaults and tests.
- **Add a new intent**: update the registry + docs + tests.

## Testing / verification

- Unit tests: `tests/unit/test_registry.py`, `tests/unit/test_vector_steps.py`, `tests/unit/test_transformations_base.py`
- Integration coverage (when Docker stack is up): transformation execution against real PostGIS.

## Links

- Root guide: `../../AGENTS.md`
- Dagster orchestration: `../../services/dagster/AGENTS.md`
