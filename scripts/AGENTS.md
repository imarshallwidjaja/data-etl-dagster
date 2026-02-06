# scripts — Agent Guide

## Scope
Utility scripts for local dev and CI.

## Key invariants
- Scripts must run in CI and locally (assume Docker is available, not always running).
- Exit codes matter for CI.
- If importing `libs/`, ensure it is installed in the environment.

## Docker compose invocation
- Dev stack: `docker compose up -d` (auto-loads `compose.override.yaml`).
- Test stack: `docker compose -f compose.yaml -f compose.test.yaml -p <project> up -d --build`.
  The explicit `-f` flags prevent auto-loading `compose.override.yaml`, avoiding
  port and container-name conflicts when the dev stack is already running.

## References
- Scripts README: `scripts/README.md`
- Testing guide: `docs/agents/testing.md`
