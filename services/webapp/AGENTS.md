# services/webapp/ — Agent Guide

## What this directory is / owns

This directory owns the **tooling webapp**: a FastAPI-based web interface for managing the data-etl-dagster pipeline without direct access to Dagster, MongoDB, or MinIO.

## Key invariants / non-negotiables

- **Webapp is stateless**: SQLite is for ephemeral session data only. MongoDB/MinIO are the source of truth.
- **Auth abstraction**: `AuthProvider` interface enables future SSO without changing dependencies.
- **No direct lake writes**: All data flows through the pipeline via manifest creation.
- **Libs reuse**: Use `libs/models` for manifest/asset validation (consistency with pipeline).
- **Mixed response format**: All list endpoints support `?format=json` for API access.

### Manifest schema endpoint (contract)

- **Route**: `GET /manifests/schemas/{asset_type}`
- **asset_type values**: `spatial`, `tabular`, `joined`
- **Response**: JSON Schema generated via `ManifestCreateRequest.model_json_schema()` (Pydantic v2)
- **Extension field**: `x-asset-type` added at schema root with the requested asset type
- **Compatibility**: Must remain Ajv-consumable JSON Schema (draft-07)
- **Error behavior**: Returns 400 for invalid asset_type

### Client-side validation (contract)

- `static/js/validation.js` provides `ManifestFormValidator` class
- Templates must:
  - Include Ajv CDN **before** `validation.js` (loaded in `base.html`)
  - Initialize validator with `new ManifestFormValidator(schemaUrl, formId, submitBtnId)`
- Validator assumptions:
  - Input `name` attributes align with schema property names
  - Server-side validation remains authoritative (client-side is UX only)
- Graceful degradation: If Ajv fails to load, form submits normally (server validates)

### Operational dependency

- Ajv is loaded via CDN in `base.html`: `https://cdn.jsdelivr.net/npm/ajv@8/dist/ajv.bundle.min.js`
- Offline deployments must vendor Ajv locally and update `base.html`

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
  - `workflows.py` - Guided wizard-style workflows
  - `runs.py` - Dagster run tracking
  - `assets.py` - Asset browsing & lineage
- `app/services/` - Service wrappers
  - `minio_service.py` - MinIO operations
  - `mongodb_service.py` - MongoDB queries
  - `dagster_service.py` - Dagster GraphQL
  - `manifest_builder.py` - Form → Manifest conversion
  - `workflow_registry.py` - Workflow definitions and steps
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
| GET | `/manifests/schemas/{asset_type}` | ✅ | JSON Schema for form validation |
| GET | `/manifests/{batch_id}` | ✅ | Manifest details |
| POST | `/manifests/{batch_id}/rerun` | ✅ | Re-run manifest |
| GET | `/workflows/` | ✅ | List available workflows |
| GET | `/workflows/{id}` | ✅ | Start workflow wizard |
| POST | `/workflows/{id}/step/{n}` | ✅ | Process workflow step (HTMX) |
| GET | `/runs/` | ✅ | List Dagster runs |
| GET | `/runs/{run_id}` | ✅ | Run details + events |
| GET | `/assets/` | ✅ | List assets |
| GET | `/assets/{dataset_id}` | ✅ | Asset versions |
| GET | `/assets/{id}/v{ver}/download` | ✅ | Download asset |
| GET | `/assets/{id}/v{ver}/lineage` | ✅ | View lineage |
| GET | `/assets/{id}/v{ver}/lineage/graph` | ✅ | Lineage graph JSON for Cytoscape.js |

## Enhanced Asset Display

- **Markdown rendering**: Only `metadata.description` is rendered as markdown and sanitized with `bleach`.
- **CRS/bounds**: Read from top-level `asset.crs` and `asset.bounds`, not metadata.
- **Column schema**: Display `ColumnInfo` fields (title, description, type_name, logical_type, nullable).

## Phase 5 UI Polish

- **Font Size**: Base font size set to `87.5%` (14px equivalent) to increase information density, with corresponding spacing adjustments.
- **Badge System**: Mapping for statuses and kinds uses `badge-unknown` as fallback and `badge-kind-*` for asset-specific styling.
- **Accessibility**:
  - Skip link added (`#main-content`) for keyboard navigation.
  - `aria-live="polite"` implemented on run status fragments for screen reader updates.
- **Mobile Responsiveness**:
  - Tables wrapped in `figure` with `overflow-x: auto` for horizontal scrolling.
  - Mobile-specific grid and navigation adjustments.
- **Utilities**: `btn-sm` utility added for compact action buttons.

## Testing

### Unit tests (in `../../tests/unit/webapp/`):
- `test_auth.py` - Auth provider tests
- `test_manifest_builder.py` - Form → model tests
- `test_rerun_versioning.py` - Batch ID versioning
- `test_folder_router.py` - Folder CRUD tests
- `test_minio_service.py` - MinIO service mocking
- `test_manifest_router.py` - Manifest router tests
- `test_manifest_schema.py` - Schema endpoint tests (14 tests)
- `test_workflow_registry.py` - Registry definitions
- `test_workflow_router.py` - Wizard logic and state

### Integration tests (in `../../tests/integration/`):
- `test_webapp_health.py` - Health endpoint tests
- `test_webapp_landing.py` - Landing zone CRUD
- `test_webapp_assets.py` - Asset listing
- `test_webapp_workflows.py` - Wizard flow end-to-end

### Run tests:
```powershell
# Unit tests
pytest tests/unit/webapp -v

# Integration tests (Docker stack must be running)
pytest -m integration tests/integration/test_webapp*.py -v
```

## Links

- Parent guide: `../../AGENTS.md`
- Dagster: `../dagster/AGENTS.md`
- Implementation plan: `../../tmp/webapp-implementation-plan.md`
