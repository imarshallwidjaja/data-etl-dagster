# Test Suite Context

## Overview

The testing strategy for the Spatial ETL Pipeline is divided into two distinct layers: **Unit Tests** (fast, mocked) and **Integration Tests** (comprehensive, live services).

## Directory Structure

```

tests/
├── CONTEXT.md           # This file
├── conftest.py          # Shared fixtures (Pydantic models, sample data)
├── unit/                # Isolated tests (No Docker required)
│   ├── libs/            # Test shared library logic
│   ├── ops/             # Test Dagster op logic (mocked context)
│   ├── resources/       # Test resource wrappers (mocked clients)
│   └── sensors/         # Test sensor logic
└── integration/         # Live service tests (Docker required)
├── test_minio.py
├── test_mongodb.py
├── test_postgis.py
└── test_dagster.py

````

## Configuration

- **Framework:** `pytest`
- **Configuration:** `pytest.ini` (defines markers, paths, and options)
- **Dependencies:** `requirements-test.txt` (extends base requirements with testing tools like `mongomock`, `requests`, `minio`).

## Running Tests

### 1. Unit Tests (Fast)
Runs business logic validation without spinning up containers.
```bash
pytest tests/unit
````

### 2. Integration Tests (Full Stack)

Requires the Docker environment to be running.

```bash
# 1. Start services
docker compose up -d

# 2. Wait for health
python scripts/wait_for_services.py

# 3. Run tests
pytest -m integration tests/integration
```

### 3. CI/CD Pipeline

Tests are automated via GitHub Actions (`.github/workflows/integration.yml`).

  - **Unit Tests:** Run on every push/PR involving code changes.
  - **Integration Tests:** Run on infrastructure or code changes, spinning up ephemeral Docker services.