# Unit Test Context

## Philosophy

**Speed and Isolation.** Unit tests must run quickly (<10s) and require **no external services**.

## Scope

1.  **Data Models:** Validate Pydantic schemas, regex patterns (SHA256, S3 keys), and custom types (CRS, Bounds).
2.  **Transformation Logic:** Verify SQL generation strings and recipe logic without connecting to a database.
3.  **Op Logic:** Test Dagster ops (`load`, `transform`, `export`) by mocking the `OpExecutionContext` and resources.
4.  **Resource Wrappers:** Verify that resources call their underlying clients (boto3, pymongo, subprocess) correctly using mocks.
5.  **Sensors:** Verify logic for new manifest detection and cursor updates.

## Mocking Standards

- **Libraries:** Use `unittest.mock` (MagicMock, patch) for most external calls.
- **MongoDB:** Use `mongomock` to simulate database operations in memory.
- **Filesystem:** Use `tmp_path` fixture or `tempfile` mocks.
- **GDAL:** Mock `subprocess.run` to simulate CLI outputs; do not require GDAL installation on the host runner.

## Key Fixtures

Shared fixtures are defined in `tests/conftest.py`:
- `valid_manifest` / `valid_asset`: Standardized Pydantic models for happy-path testing.
- `valid_file_entry`: Helper for constructing manifest files.

## Running
```bash
pytest tests/unit -v
````