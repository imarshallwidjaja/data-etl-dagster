# services/webapp/ — Agent Guide

## What this directory is / owns

This directory owns the **tooling webapp**: a FastAPI-based web interface for managing the data-etl-dagster pipeline without direct access to Dagster, MongoDB, or MinIO.

## Key invariants / non-negotiables

- **Webapp is stateless**: SQLite is for ephemeral session data only. MongoDB/MinIO are the source of truth.
- **Auth abstraction**: `AuthProvider` interface enables future SSO without changing dependencies.
- **No direct lake writes**: All data flows through the pipeline via manifest creation.
- **Libs reuse**: Use `libs/models` for manifest/asset validation (consistency with pipeline).
- **Mixed response format**: All list endpoints support `?format=json` for API access.

## Entry points / key files

- `app/main.py` - FastAPI application entry point
- `app/config.py` - Pydantic Settings from environment variables
- `app/auth/` - Authentication module
  - `providers.py` - Abstract `AuthProvider` + `BasicAuthProvider`
  - `dependencies.py` - FastAPI `get_current_user()` dependency
- `app/routers/` - API endpoints
  - `health.py` - `/health`, `/ready`, `/whoami`
  - `landing.py` - Landing zone management
  - `manifests.py` - Manifest CRUD & re-run
  - `runs.py` - Dagster run tracking
  - `assets.py` - Asset browsing & lineage
- `app/services/` - Service wrappers
  - `minio_service.py` - MinIO operations
  - `mongodb_service.py` - MongoDB queries
  - `dagster_service.py` - Dagster GraphQL
  - `manifest_builder.py` - Form → Manifest conversion
- `app/templates/` - Jinja2 templates with PicoCSS
- `app/static/` - CSS and JavaScript
- `Dockerfile` - Python 3.11-slim, installs `libs/`

## How to work here

- **Add a new endpoint**: Create router under `app/routers/`, register in `app/main.py`
- **Add a new service**: Create under `app/services/`, inject via FastAPI dependencies
- **Modify templates**: Edit Jinja2 templates under `app/templates/`
- **Change auth**: Implement new provider in `app/auth/providers.py`

## Runtime configuration

Environment variables (via `app/config.py`):

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ENDPOINT` | `minio:9000` | MinIO host:port (no scheme) |
| `MINIO_ROOT_USER` | `minio` | MinIO access key |
| `MINIO_ROOT_PASSWORD` | `minio_password` | MinIO secret key |
| `MONGO_CONNECTION_STRING` | see .env | MongoDB connection URI |
| `DAGSTER_GRAPHQL_URL` | `http://dagster-webserver:3000/graphql` | Dagster GraphQL endpoint |
| `WEBAPP_USERNAME` | `admin` | HTTP Basic Auth username |
| `WEBAPP_PASSWORD` | `admin` | HTTP Basic Auth password |

## Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | ❌ | Container health check |
| GET | `/ready` | ❌ | Service readiness probe |
| GET | `/whoami` | ✅ | Return authenticated user |
| GET | `/` | ✅ | Index page (HTML) |
| GET | `/landing/` | ✅ | Landing zone file browser |
| POST | `/landing/upload` | ✅ | Upload file |
| GET | `/landing/download/{path}` | ✅ | Download file |
| POST | `/landing/delete/{path}` | ✅ | Delete file |
| GET | `/manifests/` | ✅ | List manifests |
| GET | `/manifests/new` | ✅ | Asset type selection |
| GET | `/manifests/new/{type}` | ✅ | Asset-specific form |
| POST | `/manifests/new/{type}` | ✅ | Create manifest |
| GET | `/manifests/{batch_id}` | ✅ | Manifest details |
| POST | `/manifests/{batch_id}/rerun` | ✅ | Re-run manifest |
| GET | `/runs/` | ✅ | List Dagster runs |
| GET | `/runs/{run_id}` | ✅ | Run details + events |
| GET | `/assets/` | ✅ | List assets |
| GET | `/assets/{dataset_id}` | ✅ | Asset versions |
| GET | `/assets/{id}/v{ver}/download` | ✅ | Download asset |
| GET | `/assets/{id}/v{ver}/lineage` | ✅ | View lineage |

## Testing

### Unit tests (in `tests/unit/`):
- `test_auth.py` - Auth provider tests
- `test_manifest_builder.py` - Form → model tests
- `test_rerun_versioning.py` - Batch ID versioning

### Integration tests (in `../../tests/integration/`):
- `test_webapp_health.py` - Health endpoint tests
- `test_webapp_landing.py` - Landing zone CRUD
- `test_webapp_assets.py` - Asset listing

### Run tests:
```powershell
# Unit tests
pytest services/webapp/tests/unit -v

# Integration tests (Docker stack must be running)
pytest -m integration tests/integration/test_webapp*.py -v
```

## Links

- Parent guide: `../../AGENTS.md`
- Dagster: `../dagster/AGENTS.md`
- Implementation plan: `../../tmp/webapp-implementation-plan.md`
