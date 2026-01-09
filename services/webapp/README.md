# Tooling Webapp

FastAPI-based web interface for managing the data-etl-dagster pipeline.

## Overview

This webapp provides a user-friendly interface for:

- **Landing Zone Management** - Browse, upload, delete files in the landing zone
- **Manifest Creation** - Asset-type-specific forms with validation
- **Manifest Re-run** - Re-process archived manifests with versioned batch IDs
- **Run Tracking** - Monitor Dagster run progress with error diagnostics
- **Asset Browsing** - View metadata, versions, lineage, and download assets

## Quick Start

### Start the Webapp

```powershell
# Build and start
docker compose up -d --build webapp

# Or start with all dependencies
docker compose -f docker-compose.yaml up -d dagster-webserver dagster-daemon user-code minio minio-init mongodb postgis dagster-postgres webapp
```

### Access

| URL | Credentials |
|-----|-------------|
| http://localhost:8080 | `admin:admin` (default) |

### Health Check

```powershell
# Health endpoint (no auth)
curl http://localhost:8080/health

# Authenticated endpoint
curl -u admin:admin http://localhost:8080/whoami
```

## Configuration

Set environment variables in `.env` or `docker-compose.yaml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `WEBAPP_USERNAME` | `admin` | HTTP Basic Auth username |
| `WEBAPP_PASSWORD` | `admin` | HTTP Basic Auth password |
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
| Authentication | HTTP Basic Auth (extensible) |

## Directory Structure

```
services/webapp/
├── Dockerfile
├── requirements.txt
├── AGENTS.md              # AI agent context
├── README.md              # This file
└── app/
    ├── main.py            # FastAPI entry point
    ├── config.py          # Pydantic Settings
    ├── auth/              # Authentication module
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

## Development

### Rebuild After Code Changes

```powershell
docker compose build webapp
docker compose up -d webapp
docker compose logs -f webapp
```

### Run Tests

```powershell
# Activate conda environment
conda activate data-etl-dagster

# Unit tests (in root tests/unit/webapp/)
pytest tests/unit/webapp -v

# Integration tests (Docker stack must be running)
pytest -m integration tests/integration/test_webapp*.py -v
```

## Implementation Status

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 | Foundation (auth, health) | ✅ Complete |
| Phase 2 | Service wrappers | ✅ Complete |
| Phase 3 | API endpoints | ✅ Complete |
| Phase 4 | Templates & forms | ✅ Complete |
| Phase 5 | Testing & docs | ✅ Complete |

## Schema-Driven Form Validation

The webapp uses JSON Schema for client-side form validation before server submission.

### How It Works

1. Backend serves JSON Schema via `GET /manifests/schemas/{asset_type}`
2. Frontend loads schema and validates using Ajv library
3. Inline errors display on blur and form submit
4. Server-side validation remains authoritative (client-side is UX enhancement)

### Using the Schema Endpoint

```bash
# Get schema for spatial manifest form
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
