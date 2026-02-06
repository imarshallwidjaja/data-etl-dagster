# Testing Guide

## Unit tests
- Environment: `uv sync --frozen --group test` (creates `.venv` with Python 3.10).
- Command: `uv run pytest tests/unit`
- Legacy alternative (still works): `pip install -r requirements-test.txt && pytest tests/unit`

## Integration tests
- Require Docker services.
- Command (host): `uv run pytest -m "integration" tests/integration`
- Command (in-network): see _Docker test stack_ below.

## E2E tests
- Require Docker services.
- Command (host): `uv run pytest -m "integration and e2e" tests/integration`
- Command (in-network): see _Docker test stack_ below.

## Agent-safe wrapper (preferred for agents)

The `worktree_stack.py` wrapper automates project naming and state tracking:

```
uv run python scripts/worktree_stack.py up
uv run python scripts/worktree_stack.py test -- -q --tb=short
uv run python scripts/worktree_stack.py down
```

- Derives a deterministic compose project name (`wt-<sha256[:8]>`) from the
  worktree root so multiple stacks can coexist without port or name conflicts.
- Tracks stack state in `<worktree>/.worktree/stack.json`; `down` refuses
  without it (prevents tearing down the wrong stack).
- Under the hood it executes the same explicit `docker compose -f ... -p ...`
  commands documented in _Docker test stack_ below.

## Cleanup expectations
- Integration/E2E tests should clean artifacts they create (manifests, runs, assets).
- Set `PRESERVE_TEST_RUNS=1` to keep run records and activity logs for debugging.
- Cleanup must be performed within the test/fixture (use `try/finally`). Pre-clean fixtures do not do post-test cleanup.

## Dagster run tagging for tests
- GraphQL-launched runs: include `executionMetadata.tags` with `testing=true` (use `build_test_run_tags` in `tests/integration/helpers.py`).
- Manifest-driven sensors: set `metadata.tags.testing=true` so sensors propagate `testing=true` on RunRequests.

## Service readiness helpers
- `uv run python scripts/wait_for_services.py`
- `uv run python scripts/check_container_stability.py`
- For project-scoped stacks: `COMPOSE_PROJECT_NAME=<project> uv run python scripts/check_container_stability.py`

## Docker stack (local dev)
Start the stack:

```
docker compose up -d --build \
    dagster-webserver dagster-daemon user-code minio minio-init mongodb postgis dagster-postgres
```

Stop the stack:

```
docker compose down -v
```

## Docker test stack (in-network test-runner)

The test overlay (`compose.test.yaml`) adds a `test-runner` container that
executes pytest inside the Docker network.  Explicit `-f` flags prevent
auto-loading `compose.override.yaml`, so no host ports are exposed and the
test stack can run alongside the dev stack under a separate `-p` project name.

Start the test stack (builds test-runner and services):

```
docker compose -f compose.yaml -f compose.test.yaml -p <project> up -d --build
```

Run tests inside the network:

```
docker compose -f compose.yaml -f compose.test.yaml -p <project> run --rm test-runner
```

Override the default command (unit tests) to run integration tests:

```
docker compose -f compose.yaml -f compose.test.yaml -p <project> run --rm test-runner \
    uv run pytest tests/integration -m integration -q
```

Tear down the test stack:

```
docker compose -f compose.yaml -f compose.test.yaml -p <project> down -v
```

Some notes:
- Replace `<project>` with a unique name (e.g. `wt-smoke`, branch slug) to
  avoid collisions with the dev stack.
- The `-f` flag is required; without it, Compose auto-loads
  `compose.override.yaml` which exposes host ports and sets `container_name`,
  causing conflicts when another stack is already running.
