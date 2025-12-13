# Integration Test Context

## Philosophy

**Reality Check.** Integration tests verify that the application correctly interacts with live services running in the Docker network.

## Scope

1.  **Connectivity:** Can the application actually reach MinIO, MongoDB, PostGIS, and Dagster?
2.  **Service Operations:**
    - **MinIO:** Bucket creation, object read/write, presigned URL generation.
    - **MongoDB:** Document CRUD, index enforcement, unique constraint checks.
    - **PostGIS:** Ephemeral schema creation/deletion, extension availability, basic spatial queries (`ST_MakePoint`).
    - **Dagster:** GraphQL API availability and job submission.
3.  **Environment Config:** Verifies that `.env` settings map correctly to actual connections.

## Prerequisites

These tests **will fail** if the Docker stack is not running or healthy.

1.  **Start Environment:**
    ```bash
    docker compose up -d
    ```
2.  **Verify Health:**
    Use the helper script to ensure all services are listening before running tests.
    ```bash
    python scripts/wait_for_services.py
    ```

## Running

Target the integration marker to exclude unit tests if desired.
```bash
pytest -m integration tests/integration -v
````

## Architectural Laws Verified

  - **Transient Compute:** Tests verify that PostGIS ephemeral schemas are created and successfully dropped.
  - **Persistence:** Tests verify that data written to MinIO can be read back and that MongoDB enforces schema validation.
