# configs/ — Agent Guide

## What this directory is / owns

This directory is reserved for **configuration templates** and shared config artifacts for the platform.
**Current state:** it only contains this guide.

## Key invariants / non-negotiables

- **No secrets in-repo**: do not commit credentials here.
- **Runtime config comes from environment variables** (see `docker-compose.yaml`).
- **Canonical config model definitions live in code**: `libs/models/config.py`.

## Entry points / key files

- `../docker-compose.yaml` (source of truth for dev env vars)
- `../libs/models/config.py` (Pydantic Settings models + env var aliases)
- `AGENTS.md` (this file)

## How to work here

If/when you add real templates (e.g. logging configs), keep them:

- minimal and environment-agnostic
- safe to commit (no secrets)
- referenced from code/docs with stable paths

## Common tasks

- **Add a new config template**: add the file here, document its purpose, and ensure the runtime still uses env vars.
- **Update settings documentation**: update `libs/models/AGENTS.md` (not this directory).

## Testing / verification

None specific (this directory should remain non-executable).

## Links

- Root guide: `../AGENTS.md`
- Settings/models: `../libs/models/AGENTS.md`
