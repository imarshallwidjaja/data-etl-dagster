# libs/transformations/ — Agent Guide

## What this directory is / owns

This library implements the **recipe-based transformation architecture** for the ETL pipeline.
It maps a manifest `intent` to a sequence of transformation steps executed in order.

> **Rule**: Data transformations MUST be defined as recipes in this package, not inline in ops/assets.

## Key invariants / non-negotiables

- **Recipe-first**: all transformations must go through the recipe system for consistency and testability.
- **Steps are stateless**: configuration via constructor args only.
- **Deterministic**: same inputs → same outputs.
- **Identifier safety**: validate schema/table/column names before use.

## Entry points / key files

- `registry.py`: `RecipeRegistry` (intent → list[steps])
- `vector.py`: vector/spatial transformation steps (CRS, simplification, spatial index)
- `base.py`: base step interface

## Current recipes

| Intent | Description |
|--------|-------------|
| `ingest_vector` | Default spatial: CRS normalize → simplify → index |
| `ingest_building_footprints` | Stronger simplification for building data |
| (fallback) | Unknown intents use default recipe |

## How to work here

- **Add a new step**: extend `VectorStep` in `vector.py`, add unit tests
- **Add/modify a recipe**: update `RecipeRegistry` in `registry.py`, add tests

## Testing / verification

- `tests/unit/libs/test_registry.py`
- `tests/unit/libs/test_vector_steps.py`
- `tests/unit/libs/test_transformations_base.py`

## Links

- Root guide: `../../AGENTS.md`
- Dagster orchestration: `../../services/dagster/AGENTS.md`
- PostGIS service: `../../services/postgis/AGENTS.md`
