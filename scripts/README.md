# Scripts

Utility scripts for development, CI/CD, and operations.

## Quick Reference

| Script | Purpose | When to Use |
|--------|---------|-------------|
| `wait_for_services.py` | Wait for all services to be healthy | Before running tests |
| `check_container_stability.py` | Detect container restart loops | CI/CD pipelines |
| `migrate_db.py` | Run MongoDB schema migrations | After schema changes |

---

## wait_for_services.py

Polls all service endpoints until they become ready or timeout.

### Usage

```bash
# Default (60s timeout per service, localhost URLs)
uv run python scripts/wait_for_services.py

# Custom timeout
SERVICE_WAIT_TIMEOUT=120 uv run python scripts/wait_for_services.py

# In-network execution (e.g. inside test-runner container)
DAGSTER_GRAPHQL_URL=http://dagster-webserver:3000/graphql \
WEBAPP_URL=http://webapp:8080 \
  uv run python scripts/wait_for_services.py
```

### What it checks

1. **MinIO** - Can list buckets
2. **MongoDB** - Admin ping succeeds
3. **PostGIS** - PostGIS extension is installed
4. **Dagster** - GraphQL API responds
5. **User-code** - `gdal_health_check_job` is registered (proves code loaded)
6. **Webapp** - `/health` endpoint returns `{"status": "healthy"}`

### Output

```
============================================================
Service Health Check
============================================================
Waiting for MinIO...
[OK] MinIO is ready (took 0.5s)
Waiting for MongoDB...
[OK] MongoDB is ready (took 0.3s)
...
============================================================
SUCCESS: All services are ready
```

### Exit codes

- `0` - All services ready
- `1` - One or more services failed

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVICE_WAIT_TIMEOUT` | `60` | Timeout per service (seconds) |
| `DAGSTER_WEBSERVER_PORT` | `3000` | Dagster UI port (used when `DAGSTER_GRAPHQL_URL` is unset) |
| `DAGSTER_GRAPHQL_URL` | — | Full Dagster GraphQL URL (takes priority over port) |
| `WEBAPP_URL` | `http://localhost:8080` | Full webapp base URL |
| `WAIT_FOR_SERVICES` | (all) | Comma-separated list of services to check |

---

## check_container_stability.py

Monitors container restart counts over a period to detect restart loops.

### Usage

```bash
# Default (30s monitoring window, current compose project)
uv run python scripts/check_container_stability.py

# Custom monitoring duration
CONTAINER_STABILITY_MONITOR_DURATION=60 uv run python scripts/check_container_stability.py

# Project-scoped stack (e.g. worktree test stack)
COMPOSE_PROJECT_NAME=wt-smoke uv run python scripts/check_container_stability.py
```

### What it monitors

By default, derives container names as `<project>-<service>-1`, where `<project>` is:

- `COMPOSE_PROJECT_NAME` when set, otherwise
- Docker Compose's default project name (current directory basename)

Default monitored services:

- `dagster-webserver` → `<project>-dagster-webserver-1`
- `dagster-daemon` → `<project>-dagster-daemon-1`
- `user-code` → `<project>-user-code-1`
- `mongodb` → `<project>-mongodb-1`
- `postgis` → `<project>-postgis-1`
- `minio` → `<project>-minio-1`

For project-scoped stacks, set `COMPOSE_PROJECT_NAME` explicitly so this script
targets the same stack.

### How it works

1. Records initial restart count for each container
2. Waits for monitoring duration
3. Compares final restart count
4. Fails if any container restarted during the window

### Output

```
============================================================
Container Stability Check
============================================================
  Monitoring containers for 30 seconds...

  <project>-dagster-webserver-1: initial restart count = 0
  <project>-dagster-daemon-1: initial restart count = 0
  ...

Waiting 30 seconds...

[OK] <project>-dagster-webserver-1: stable (restart count = 0)
[OK] <project>-dagster-daemon-1: stable (restart count = 0)
...
============================================================
SUCCESS: All containers are stable
```

### Exit codes

- `0` - All containers stable
- `1` - One or more containers unstable

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CONTAINER_STABILITY_MONITOR_DURATION` | `30` | Monitoring window (seconds) |
| `COMPOSE_PROJECT_NAME` | current directory basename | Compose project name used to derive `<project>-<service>-1` |
| `CHECK_CONTAINERS` | — | Comma-separated list of explicit container names (overrides project-derived names) |

---

## migrate_db.py

Runs MongoDB schema migrations in order.

### Usage

```bash
# Run all pending migrations
uv run python scripts/migrate_db.py
```

### How it works

1. Discovers `NNN_*.py` files in `services/mongodb/migrations/`
2. Checks `schema_migrations` collection for applied versions
3. Applies pending migrations in order
4. Records successful migrations with timestamp

### Output

```
Discovered 2 migration(s)
Skipping migration 001: already applied
Applying migration 002 from 002_add_tabular_contracts.py...
Applied migration 002 (took 245ms)
All migrations applied successfully
```

### Exit codes

- `0` - All migrations applied
- `1` - Migration failed (will retry on next run)

### Environment variables

Uses standard MongoDB env vars (see `.env` file):
- `MONGO_HOST`, `MONGO_PORT`, `MONGO_DATABASE`
- `MONGO_INITDB_ROOT_USERNAME`, `MONGO_INITDB_ROOT_PASSWORD`

---

## Prerequisites

All scripts require test dependencies (uv-first):

```bash
uv sync --frozen --group test
```

Then run scripts via `uv run python scripts/<script>.py`.

---

## CI/CD Integration

These scripts are used in the GitHub Actions workflow:

```yaml
# .github/workflows/ci.yml
- name: Wait for services
  run: uv run python scripts/wait_for_services.py

- name: Check container stability
  run: uv run python scripts/check_container_stability.py
```

---

## Troubleshooting

### "Missing spatial-etl-libs package"

```bash
uv sync --frozen --group test
```

### Script hangs on a service

Check if the service container is running:

```bash
docker compose ps
docker logs <container-name>
```

### Container marked as unstable

View container logs to see crash cause:

```bash
docker compose logs user-code --tail 100
```
