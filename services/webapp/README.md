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
