# Tooling Webapp

FastAPI-based web interface for managing the data-etl-dagster pipeline.

## Overview

This webapp provides a user-friendly interface for:

- **Landing Zone Management** - Browse, upload, delete files in the landing zone
- **Manifest Creation** - Intent-specific forms with validation
- **Manifest Re-run** - Re-process archived manifests
- **Run Tracking** - Monitor Dagster run progress with error diagnostics
- **Asset Browsing** - View metadata, lineage, and download processed assets

## Quick Start

### Start the Webapp

```powershell
# Build and start (with dependencies)
docker compose up -d --build webapp

# Or start only webapp (if dependencies are running)
docker compose up -d webapp
```

### Access

| URL | Credentials |
|-----|-------------|
| http://localhost:8080 | `admin:admin` (default) |

### Health Check

```powershell
# Health endpoint (no auth)
Invoke-WebRequest -Uri "http://localhost:8080/health" -UseBasicParsing

# Authenticated endpoint
$cred = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("admin:admin"))
Invoke-WebRequest -Uri "http://localhost:8080/whoami" -Headers @{Authorization="Basic $cred"} -UseBasicParsing
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
    ├── services/          # Service wrappers
    ├── templates/         # Jinja2 templates
    ├── static/            # CSS, JS
    └── models/            # SQLite models (ephemeral)
```

## API Endpoints

### Health (No Auth)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Returns `{"status":"healthy","version":"0.1.0"}` |
| GET | `/ready` | Returns service connectivity status |

### User (Auth Required)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Index page with navigation |
| GET | `/whoami` | Returns authenticated user info |

### Coming Soon (Phase 2-4)

- `/landing/` - Landing zone management
- `/manifests/` - Manifest CRUD & re-run
- `/runs/` - Dagster run tracking
- `/assets/` - Asset browsing & lineage

## Development

### Local Development

```powershell
# Rebuild after code changes
docker compose build webapp
docker compose up -d webapp

# View logs
docker compose logs -f webapp
```

### Dependencies

The Dockerfile installs `libs/` from the repo root for shared model validation:

```dockerfile
COPY libs /opt/webapp/libs
RUN pip install --no-cache-dir -e /opt/webapp/libs
```

## Implementation Status

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 | Foundation (auth, health) | ✅ Complete |
| Phase 2 | Service wrappers | 🔲 Pending |
| Phase 3 | API endpoints | 🔲 Pending |
| Phase 4 | Templates & forms | 🔲 Pending |
| Phase 5 | Testing & docs | 🔲 Pending |
