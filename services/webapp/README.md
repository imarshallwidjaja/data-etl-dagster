# Tooling Webapp

FastAPI-based web interface for managing the data-etl-dagster pipeline.

## Overview

This webapp provides a user-friendly interface for:

- **Landing Zone Management** - Browse, upload, delete files in the landing zone
- **Guided Workflows** - Step-by-step wizards for common data operations
- **Manifest Creation** - Asset-type-specific forms with validation (supports JSON paste mode)
- **Manifest Re-run** - Re-process archived manifests with versioned batch IDs
- **Run Tracking** - Monitor Dagster run progress with error diagnostics
- **Asset Browsing** - View metadata, versions, lineage, and download assets
- **Activity Log** - Audit trail of all platform operations with user and IP tracking


## Quick Start

### Start the Webapp

```powershell
# Build and start
docker compose up -d --build webapp

# Or start with all dependencies
docker compose up -d dagster-webserver dagster-daemon user-code minio minio-init mongodb postgis dagster-postgres webapp
```

### Access

| URL | Credentials |
|-----|-------------|
| http://localhost:8080 | `admin:admin` (default, via form login) |

Navigate to http://localhost:8080 — unauthenticated requests redirect to `/login` where you enter credentials. On success you receive a signed session cookie that authenticates subsequent requests.

### Health Check

```powershell
# Health endpoint (no auth)
curl http://localhost:8080/health

# Authenticated endpoint (hybrid mode allows Basic auth fallback)
curl -u admin:admin http://localhost:8080/whoami
```

## Configuration

Set environment variables in `.env` or `compose.yaml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT` | `development` | Runtime environment (`development`, `ci`, `staging`, `production`) |
| `WEBAPP_USERNAME` | `admin` | Auth username |
| `WEBAPP_PASSWORD` | `admin` | Auth password |
| `WEBAPP_AUTH_MODE` | `session` | `session` or `hybrid` (session + Basic fallback) |
| `WEBAPP_SESSION_SECRET` | _(none)_ | Signing key for session cookies. **Required** outside `development`. |
| `WEBAPP_SESSION_MAX_AGE_SECONDS` | `28800` | Absolute session lifetime (seconds) |
| `WEBAPP_SESSION_SECURE` | `false` | Set `true` to emit `Secure` cookie flag (requires HTTPS) |
| `WEBAPP_SESSION_COOKIE_NAME` | `webapp_session` | Name of the session cookie |
| `MINIO_ENDPOINT` | `minio:9000` | MinIO host:port |
| `MONGO_CONNECTION_STRING` | (see .env) | MongoDB connection URI |
| `DAGSTER_GRAPHQL_URL` | `http://dagster-webserver:3000/graphql` | Dagster GraphQL |

## Technology Stack

| Component | Technology |
|-----------|------------|
| Backend | FastAPI (Python 3.11) |
| Frontend | Jinja2 templates |
| CSS | PicoCSS (semantic, dark theme) |
| JavaScript | Vanilla JS (form interactivity) |
| Authentication | Signed session cookies (form login) with optional HTTP Basic fallback |

## Directory Structure

```
services/webapp/
├── Dockerfile
├── AGENTS.md              # AI agent context
├── README.md              # This file
└── app/
    ├── main.py            # FastAPI entry point
    ├── config.py          # Pydantic Settings
    ├── auth/              # Authentication (providers, session helpers, dependencies)
    ├── security/          # CSRF enforcement
    ├── routers/           # API endpoints
    ├── services/          # Service wrappers (MinIO, MongoDB, Dagster)
    ├── templates/         # Jinja2 templates
    └── static/            # CSS, JS

# Tests are in the root tests/ directory:
tests/
├── unit/webapp/           # Webapp unit tests
└── integration/test_webapp*.py  # Webapp integration tests
```

## API Endpoints

### Auth (No Session Required)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/login` | Renders the login form (username, password, CSRF token) |
| POST | `/login` | Validates credentials, issues session cookie, redirects to `next` or `/` |
| POST | `/logout` | Clears session and redirects to `/login` (requires CSRF token) |

### Health (No Auth)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Returns `{"status":"healthy","version":"0.1.0"}` |
| GET | `/ready` | Returns service connectivity status |

### Landing Zone

| Method | Path | Description |
|--------|------|-------------|
| GET | `/landing/` | File browser (HTML) or list (`?format=json`) |
| POST | `/landing/upload` | Upload file to landing zone |
| GET | `/landing/download/{path}` | Download file |
| POST | `/landing/delete/{path}` | Delete file |

### Manifests

| Method | Path | Description |
|--------|------|-------------|
| GET | `/manifests/` | List manifests with filters |
| GET | `/manifests/new` | Asset type selection |
| GET/POST | `/manifests/new/{type}` | Asset-specific form & create |
| GET | `/manifests/{batch_id}` | Manifest details |
| POST | `/manifests/{batch_id}/rerun` | Re-run archived manifest |

### Workflows

| Method | Path | Description |
|--------|------|-------------|
| GET | `/workflows/` | List available guided workflows |
| GET | `/workflows/{id}` | Start a workflow wizard |
| GET | `/workflows/{id}/success` | Submission success page |
| POST | `/workflows/{id}/step/{n}` | Process workflow step (HTMX) |

### Runs


| Method | Path | Description |
|--------|------|-------------|
| GET | `/runs/` | List Dagster runs with status filter |
| GET | `/runs/{run_id}` | Run details with events/logs |

### Assets

| Method | Path | Description |
|--------|------|-------------|
| GET | `/assets/` | List assets with kind filter |
| GET | `/assets/{dataset_id}` | Asset versions |
| GET | `/assets/{id}/v{ver}/download` | Download asset file |
| GET | `/assets/{id}/v{ver}/lineage` | View parent assets |

### Activity

| Method | Path | Description |
|--------|------|-------------|
| GET | `/activity/` | View platform activity logs with filters (`user`, `action`, `resource_type`, `resource_id`) and pagination (`offset`, `limit`). Supports `format=json`. |

## Development

### Rebuild After Code Changes

```powershell
docker compose build webapp
docker compose up -d webapp
docker compose logs -f webapp
```

### Run Tests

```bash
# Install test dependencies
uv sync --frozen --group test

# Unit tests (in root tests/unit/webapp/)
uv run pytest tests/unit/webapp -v

# Integration tests (Docker stack must be running)
uv run pytest -m integration tests/integration/test_webapp*.py -v
```

## Core Features

- **Platform Foundation** - Authentication, health monitoring, and service readiness checks.
- **Service Integration** - Native wrappers for MinIO, MongoDB, and Dagster GraphQL.
- **Data Ingestion** - Schema-driven forms for spatial and tabular data with client-side validation.
- **Operational Control** - Manifest re-runs, batch versioning, and real-time run status tracking.
- **Asset Catalog** - Deep inspection of asset metadata, column schemas, and lineage.
- **Observability** - Comprehensive activity logging and platform-wide audit trail.


## Authentication

The webapp uses form-based login with signed session cookies. A single credential pair is configured via `WEBAPP_USERNAME` / `WEBAPP_PASSWORD` (no user database).

### Auth Modes

| Mode | Behaviour |
|------|-----------|
| `session` (default) | Session cookies are the only accepted transport. Unauthenticated browser requests redirect to `/login`. |
| `hybrid` | Session-first resolution with HTTP Basic auth fallback. Useful for API clients and backward compatibility. |

Set the mode with `WEBAPP_AUTH_MODE`.

### Login Flow

1. Unauthenticated request to a protected route → 303 redirect to `/login?next=<path>`.
2. User submits username + password via the login form (CSRF-protected).
3. On success, the server issues a signed session cookie and redirects to `next` (or `/`).
4. Session identifier is rotated on login (fixation protection).
5. Logout clears the session and redirects to `/login`.

### API / Fetch Behaviour

- Requests with `Accept: application/json`, `?format=json`, or to `/whoami` receive `401 JSON` when unauthenticated (no redirect).
- In `hybrid` mode, HTTP Basic auth is accepted as a fallback for these requests.

### CSRF Protection

All state-changing endpoints (POST/PUT/PATCH/DELETE) enforce a synchronizer CSRF token when the request has an active session. The token is accepted from:
- `X-CSRF-Token` header (fetch / HTMX)
- `csrf_token` form field (HTML forms)

A `<meta name="csrf-token">` tag in the base template and `static/js/csrf.js` auto-inject the token for `window.fetch` and HTMX requests.

### Session Cookie Properties

| Property | Value |
|----------|-------|
| Signed | Yes (via `WEBAPP_SESSION_SECRET`) |
| HttpOnly | Yes |
| SameSite | Lax |
| Secure | Configurable (`WEBAPP_SESSION_SECURE`) |
| Max Age | `WEBAPP_SESSION_MAX_AGE_SECONDS` (default 8 hours) |

### Audit Logging

Auth lifecycle events (`login_success`, `login_failure`, `logout`, `unauthorized_access`) are logged to `activity_logs` with the client IP address.

## Schema-Driven Form Validation

The webapp uses JSON Schema for client-side form validation before server submission.

### How It Works

1. Backend serves JSON Schema via `GET /manifests/schemas/{asset_type}`
2. Frontend loads schema and validates using Ajv library
3. Inline errors display on blur and form submit
4. Server-side validation remains authoritative (client-side is UX enhancement)

### Using the Schema Endpoint

```bash
# Get schema for spatial manifest form (hybrid mode with Basic auth)
curl -u admin:admin http://localhost:8080/manifests/schemas/spatial | jq

# Get schema for tabular manifest form
curl -u admin:admin http://localhost:8080/manifests/schemas/tabular | jq
```

Response includes `x-asset-type` extension field indicating the requested type.

### Template Wiring

Forms are validated using `ManifestFormValidator` from `static/js/validation.js`:

```html
<script src="/static/js/validation.js"></script>
<script>
  document.addEventListener('DOMContentLoaded', function() {
    new ManifestFormValidator(
      '/manifests/schemas/spatial',  // Schema URL
      'manifest-form',                // Form element ID
      'submit-btn'                    // Submit button ID
    );
  });
</script>
```

### Adding a New Asset Type

1. Add the asset type to the schema endpoint in `app/routers/manifests.py`
2. Create form template `app/templates/manifests/new_{type}.html`
3. Initialize `ManifestFormValidator` with the correct schema URL
4. Add tests in `tests/unit/webapp/test_manifest_schema.py`

### Troubleshooting

| Issue | Solution |
|-------|----------|
| Validation not working | Check browser console for Ajv load errors |
| CDN blocked | Vendor Ajv locally in `app/static/vendor/` |
| Schema mismatch | Verify `ManifestCreateRequest` model matches form fields |
| Field names wrong | Input `name` attributes must match schema property names |
