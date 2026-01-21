# scripts — Agent Guide

## Scope
Utility scripts for local dev and CI.

## Key invariants
- Scripts must run in CI and locally (assume Docker is available, not always running).
- Exit codes matter for CI.
- If importing `libs/`, ensure it is installed in the environment.

## References
- Scripts README: `scripts/README.md`
