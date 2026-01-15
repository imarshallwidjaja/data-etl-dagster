"""Shared helpers for integration/E2E tests.

This module consolidates common patterns used across E2E tests:
- DagsterGraphQLClient for GraphQL operations
- Polling helpers for run completion
- Error formatting for CI/CD visibility
- Dynamic partition management via GraphQL
- MongoDB assertion helpers (canonical run→asset linkage)
- MinIO/data-lake assertion helpers
- Cleanup utilities with failure context preservation
"""

from __future__ import annotations

import time
from io import BytesIO
from typing import TYPE_CHECKING, Callable, Mapping

import requests
from requests.exceptions import RequestException, Timeout

if TYPE_CHECKING:
    from minio import Minio
    from pymongo import MongoClient

    from libs.models import MinIOSettings, MongoSettings

# Type alias for JSON-like dictionaries
JsonDict = dict[str, object]


# =============================================================================
# GraphQL Client
# =============================================================================


class DagsterGraphQLClient:
    """Simple GraphQL client for Dagster API."""

    def __init__(self, url: str):
        self.url = url

    def query(
        self,
        query_str: str,
        variables: Mapping[str, object] | None = None,
        timeout: int = 30,
    ) -> JsonDict:
        """Execute a GraphQL query."""
        payload: JsonDict = {"query": query_str}
        if variables:
            payload["variables"] = variables

        try:
            response = requests.post(self.url, json=payload, timeout=timeout)
            if response.status_code != 200:
                raise RuntimeError(
                    f"GraphQL request failed with status {response.status_code}: "
                    f"{response.text}"
                )
            return response.json()
        except (RequestException, Timeout) as e:
            raise RuntimeError(
                f"Failed to communicate with Dagster GraphQL API: {e}"
            ) from e


def wait_for_graphql_ready(
    url: str, timeout: int = 30, poll_interval: float = 1.0
) -> DagsterGraphQLClient:
    """Wait for Dagster GraphQL endpoint to become ready.

    Args:
        url: GraphQL endpoint URL
        timeout: Maximum seconds to wait
        poll_interval: Seconds between attempts

    Returns:
        Ready DagsterGraphQLClient instance

    Raises:
        RuntimeError: If GraphQL doesn't become ready within timeout
    """
    deadline = time.time() + timeout
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            client = DagsterGraphQLClient(url)
            client.query("{ __typename }")
            return client
        except Exception as e:
            last_error = e
            time.sleep(poll_interval)

    raise RuntimeError(
        f"Dagster GraphQL did not become ready within {timeout}s. "
        f"Last error: {last_error}"
    )


# =============================================================================
# Dynamic Partition Management (GraphQL-based)
# =============================================================================


def add_dynamic_partition(
    client: DagsterGraphQLClient,
    partition_key: str,
    partitions_def_name: str = "dataset_id",
    repository_location: str = "etl_pipelines",
    repository_name: str = "__repository__",
) -> None:
    """Register a dynamic partition key via GraphQL.

    Required when bypassing the sensor path which normally handles partition registration.

    Args:
        client: DagsterGraphQLClient instance
        partition_key: The partition key to register
        partitions_def_name: Name of the partitions definition (default: dataset_id)
        repository_location: Repository location name
        repository_name: Repository name
    """
    mutation = """
    mutation AddDynamicPartition(
        $repositoryLocationName: String!
        $repositoryName: String!
        $partitionsDefName: String!
        $partitionKey: String!
    ) {
        addDynamicPartition(
            repositorySelector: {
                repositoryLocationName: $repositoryLocationName
                repositoryName: $repositoryName
            }
            partitionsDefName: $partitionsDefName
            partitionKey: $partitionKey
        ) {
            ... on AddDynamicPartitionSuccess { partitionsDefName partitionKey }
            ... on PythonError { message }
            ... on DuplicateDynamicPartitionError { partitionName }
        }
    }
    """
    result = client.query(
        mutation,
        variables={
            "repositoryLocationName": repository_location,
            "repositoryName": repository_name,
            "partitionsDefName": partitions_def_name,
            "partitionKey": partition_key,
        },
        timeout=10,
    )
    if "errors" in result:
        raise RuntimeError(f"Failed to add dynamic partition: {result.get('errors')}")


def delete_dynamic_partition(
    client: DagsterGraphQLClient,
    partition_key: str,
    partitions_def_name: str = "dataset_id",
    repository_location: str = "etl_pipelines",
    repository_name: str = "__repository__",
    *,
    strict: bool = False,
) -> None:
    """Delete a dynamic partition key via GraphQL.

    Use for cleanup after E2E tests to prevent partition leakage.

    Args:
        client: DagsterGraphQLClient instance
        partition_key: The partition key to delete
        partitions_def_name: Name of the partitions definition (default: dataset_id)
        repository_location: Repository location name
        repository_name: Repository name
        strict: If True, raise RuntimeError on any GraphQL error or PythonError union.
                If False (default), swallow errors for best-effort cleanup.
    """
    mutation = """
    mutation DeleteDynamicPartition(
        $repositoryLocationName: String!
        $repositoryName: String!
        $partitionsDefName: String!
        $partitionKey: String!
    ) {
        deleteDynamicPartitions(
            repositorySelector: {
                repositoryLocationName: $repositoryLocationName
                repositoryName: $repositoryName
            }
            partitionsDefName: $partitionsDefName
            partitionKeys: [$partitionKey]
        ) {
            __typename
            ... on DeleteDynamicPartitionsSuccess { partitionsDefName }
            ... on PythonError { message stack }
        }
    }
    """
    variables = {
        "repositoryLocationName": repository_location,
        "repositoryName": repository_name,
        "partitionsDefName": partitions_def_name,
        "partitionKey": partition_key,
    }

    if strict:
        result = client.query(mutation, variables=variables, timeout=10)
        _validate_delete_partition_response(result, partition_key)
    else:
        # Best-effort deletion; don't raise on errors
        try:
            client.query(mutation, variables=variables, timeout=10)
        except Exception:
            pass  # Cleanup errors are non-fatal


def _validate_delete_partition_response(result: JsonDict, partition_key: str) -> None:
    """Validate GraphQL response for delete partition mutation.

    Raises:
        RuntimeError: If response contains errors or PythonError union type.
    """
    if "errors" in result:
        raise RuntimeError(
            f"GraphQL errors while deleting partition '{partition_key}': {result}"
        )

    data = result.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(
            f"No 'data' in response while deleting partition '{partition_key}': {result}"
        )

    delete_result = data.get("deleteDynamicPartitions")
    if not isinstance(delete_result, dict):
        raise RuntimeError(
            f"Invalid deleteDynamicPartitions result for partition '{partition_key}': {result}"
        )

    typename = delete_result.get("__typename")
    if typename == "PythonError":
        raise RuntimeError(
            f"PythonError while deleting partition '{partition_key}': {result}"
        )
    if typename != "DeleteDynamicPartitionsSuccess":
        raise RuntimeError(
            f"Unexpected __typename '{typename}' while deleting partition "
            f"'{partition_key}': {result}"
        )


# =============================================================================
# Run Polling and Error Details
# =============================================================================


def poll_run_to_completion(
    client: DagsterGraphQLClient,
    run_id: str,
    *,
    max_wait: int = 900,
    poll_interval: int = 3,
) -> tuple[str, JsonDict | None]:
    """Poll until run reaches terminal state.

    Args:
        client: DagsterGraphQLClient instance
        run_id: Dagster run ID to poll
        max_wait: Maximum seconds to wait (default 900 = 15 min)
        poll_interval: Seconds between polls

    Returns:
        Tuple of (status, error_details).
        status is one of: SUCCESS, FAILURE, CANCELED, or TIMEOUT
        error_details is None for SUCCESS, otherwise contains failure info
    """
    query = """
    query GetRun($runId: ID!) {
        runOrError(runId: $runId) {
            ... on Run { id status }
            ... on RunNotFoundError { message }
        }
    }
    """

    terminal_statuses = {"SUCCESS", "FAILURE", "CANCELED"}
    deadline = time.time() + max_wait
    status = "STARTING"

    while time.time() < deadline:
        result = client.query(query, variables={"runId": run_id}, timeout=10)

        if "errors" in result:
            raise RuntimeError(f"Failed to query run status: {result.get('errors')}")

        data = result.get("data")
        if not isinstance(data, dict):
            time.sleep(poll_interval)
            continue

        run_or_error = data.get("runOrError")
        if not isinstance(run_or_error, dict) or "id" not in run_or_error:
            time.sleep(poll_interval)
            continue

        status_val = run_or_error.get("status")
        if isinstance(status_val, str) and status_val in terminal_statuses:
            if status_val != "SUCCESS":
                error_details = get_run_error_details(client, run_id)
                return status_val, error_details
            return status_val, None

        time.sleep(poll_interval)

    # Timeout
    error_details = get_run_error_details(client, run_id)
    return "TIMEOUT", error_details


def get_run_error_details(client: DagsterGraphQLClient, run_id: str) -> JsonDict | None:
    """Fetch detailed error information from Dagster GraphQL for a failed run.

    Captures the full error chain including cause and nested errors for better
    CI/CD debugging visibility.
    """
    log_query = """
    query GetRunLogs($runId: ID!) {
        logsForRun(runId: $runId) {
            __typename
            ... on EventConnection {
                events {
                    __typename
                    ... on MessageEvent { message level }
                    ... on ExecutionStepFailureEvent {
                        stepKey
                        error {
                            message
                            stack
                            errorChain {
                                error { message stack }
                                isExplicitLink
                            }
                            cause { message stack }
                        }
                    }
                    ... on EngineEvent { message level }
                    ... on RunFailureEvent { message }
                }
            }
        }
    }
    """
    try:
        log_result = client.query(log_query, variables={"runId": run_id}, timeout=30)
    except Exception as e:
        return {"error": f"Could not fetch logs: {e}"}

    data = log_result.get("data")
    if not isinstance(data, dict):
        return {"error": "Could not fetch logs", "raw": log_result}

    logs = data.get("logsForRun")
    if not isinstance(logs, dict) or logs.get("__typename") != "EventConnection":
        return {"error": "Could not fetch logs", "raw": log_result}

    events_raw = logs.get("events")
    events_list: list[JsonDict] = []
    if isinstance(events_raw, list):
        events_list = [e for e in events_raw if isinstance(e, dict)]

    failure_events = [
        e
        for e in events_list
        if "Failure" in str(e.get("__typename", "")) or e.get("level") == "ERROR"
    ]
    context_events = events_list[-20:] if len(events_list) > 20 else events_list

    return {
        "failure_events": failure_events,
        "context_events": context_events,
        "total_events": len(events_list),
    }


def format_error_details(error_details: JsonDict | None) -> str:
    """Format error details into a readable string for pytest failure output.

    Extracts the most important error information and formats it for
    easy reading in CI/CD logs.
    """
    if not error_details:
        return "No error details available"

    lines = ["\n" + "=" * 80, "DAGSTER RUN ERROR DETAILS", "=" * 80]

    failure_events = error_details.get("failure_events")
    if not isinstance(failure_events, list):
        failure_events = []

    for i, event in enumerate(failure_events, 1):
        if not isinstance(event, dict):
            continue
        event_type = event.get("__typename", "Unknown")
        lines.append(f"\n--- Failure Event {i}: {event_type} ---")

        if event_type == "ExecutionStepFailureEvent":
            lines.append(f"Step: {event.get('stepKey', 'unknown')}")
            error = event.get("error")
            if isinstance(error, dict):
                lines.append(f"\nError Message:\n{error.get('message', 'N/A')}")
                stack = error.get("stack")
                if isinstance(stack, list):
                    lines.append("\nStack Trace:")
                    for frame in stack[-10:]:
                        if isinstance(frame, str):
                            lines.append(f"  {frame.strip()}")
                error_chain = error.get("errorChain")
                if isinstance(error_chain, list):
                    for j, chain_item in enumerate(error_chain, 1):
                        if isinstance(chain_item, dict):
                            chain_error = chain_item.get("error", {})
                            if isinstance(chain_error, dict):
                                lines.append(f"\n--- Caused By ({j}) ---")
                                lines.append(
                                    f"Message: {chain_error.get('message', 'N/A')}"
                                )
                cause = error.get("cause")
                if isinstance(cause, dict) and cause.get("message"):
                    lines.append("\n--- Root Cause ---")
                    lines.append(f"Message: {cause.get('message', 'N/A')}")
        else:
            if event.get("message"):
                lines.append(f"Message: {event.get('message')}")

    lines.append("\n" + "=" * 80)
    return "\n".join(lines)


# =============================================================================
# MongoDB Assertion Helpers (Canonical run→asset linkage)
# =============================================================================


def assert_mongodb_run_exists(
    mongo_client: "MongoClient[JsonDict]",
    mongo_settings: "MongoSettings",
    dagster_run_id: str,
) -> JsonDict:
    """Assert run document exists in MongoDB and return it.

    Args:
        mongo_client: PyMongo client
        mongo_settings: MongoSettings with database name
        dagster_run_id: Dagster run ID (UUID string)

    Returns:
        Run document from MongoDB

    Raises:
        AssertionError: If run document not found
    """
    db = mongo_client[mongo_settings.database]
    run_doc = db["runs"].find_one({"dagster_run_id": dagster_run_id})
    assert run_doc is not None, (
        f"No run document found in MongoDB for dagster_run_id: {dagster_run_id}"
    )
    return run_doc


def assert_mongodb_asset_exists(
    mongo_client: "MongoClient[JsonDict]",
    mongo_settings: "MongoSettings",
    dagster_run_id: str,
) -> JsonDict:
    """Assert asset record exists in MongoDB using canonical run→asset linkage.

    This implements the canonical lookup pattern:
    1. Find run document by dagster_run_id
    2. Get MongoDB ObjectId from run document
    3. Find asset by run_id (string representation of MongoDB ObjectId)

    Args:
        mongo_client: PyMongo client
        mongo_settings: MongoSettings with database name
        dagster_run_id: Dagster run ID (UUID string)

    Returns:
        Asset document from MongoDB

    Raises:
        AssertionError: If run or asset document not found
    """
    db = mongo_client[mongo_settings.database]

    # Step 1: Find run document
    run_doc = db["runs"].find_one({"dagster_run_id": dagster_run_id})
    assert run_doc is not None, (
        f"No run document found in MongoDB for dagster_run_id: {dagster_run_id}"
    )

    # Step 2: Get MongoDB ObjectId as string
    mongodb_run_id = str(run_doc["_id"])

    # Step 3: Find asset by run_id
    asset_doc = db["assets"].find_one({"run_id": mongodb_run_id})
    assert asset_doc is not None, (
        f"No asset record found in MongoDB for run_id: {mongodb_run_id} "
        f"(Dagster run: {dagster_run_id})"
    )
    return asset_doc


def assert_mongodb_asset_exists_legacy(
    mongo_client: "MongoClient[JsonDict]",
    mongo_settings: "MongoSettings",
    dagster_run_id: str,
) -> JsonDict:
    """Assert asset record exists using legacy dagster_run_id lookup.

    Use this for ingest_job which stores dagster_run_id directly on assets.
    For asset-based jobs (spatial/tabular/join), use assert_mongodb_asset_exists().

    Args:
        mongo_client: PyMongo client
        mongo_settings: MongoSettings with database name
        dagster_run_id: Dagster run ID (UUID string)

    Returns:
        Asset document from MongoDB

    Raises:
        AssertionError: If asset document not found
    """
    db = mongo_client[mongo_settings.database]
    asset_doc = db["assets"].find_one({"dagster_run_id": dagster_run_id})
    assert asset_doc is not None, (
        f"No asset record found in MongoDB for dagster_run_id: {dagster_run_id}"
    )
    return asset_doc


# =============================================================================
# MinIO/Data Lake Helpers
# =============================================================================


def assert_datalake_object_exists(
    minio_client: "Minio",
    bucket: str,
    s3_key: str,
) -> None:
    """Assert data lake object exists and has non-zero size.

    Args:
        minio_client: MinIO client
        bucket: Bucket name
        s3_key: Object key

    Raises:
        AssertionError: If object doesn't exist or has zero size
    """
    from minio.error import S3Error

    try:
        stat = minio_client.stat_object(bucket, s3_key)
        assert stat.size is not None and stat.size > 0, (
            f"Data-lake object {s3_key} has zero size"
        )
    except S3Error as e:
        raise AssertionError(f"Data-lake object {bucket}/{s3_key} does not exist: {e}")


def upload_bytes_to_minio(
    minio_client: "Minio",
    bucket: str,
    object_key: str,
    data_bytes: bytes,
    content_type: str,
) -> None:
    """Upload bytes to MinIO.

    Args:
        minio_client: MinIO client
        bucket: Target bucket
        object_key: Object key
        data_bytes: Data to upload
        content_type: MIME content type
    """
    minio_client.put_object(
        bucket,
        object_key,
        BytesIO(data_bytes),
        length=len(data_bytes),
        content_type=content_type,
    )


# =============================================================================
# Cleanup Utilities with Failure Context Preservation
# =============================================================================


class CleanupError(Exception):
    """Exception raised when cleanup fails after a test failure.

    Preserves both the original test failure and cleanup errors.
    """

    def __init__(
        self,
        original_error: BaseException,
        cleanup_errors: list[tuple[str, Exception]],
    ):
        self.original_error = original_error
        self.cleanup_errors = cleanup_errors

        cleanup_msgs = "; ".join(f"{name}: {err}" for name, err in cleanup_errors)
        super().__init__(
            f"Test failed with: {original_error}\n"
            f"Additionally, cleanup failed: {cleanup_msgs}"
        )


def safe_cleanup(
    cleanup_funcs: list[tuple[str, Callable[[], object]]],
    *,
    original_error: BaseException | None = None,
) -> None:
    """Execute cleanup functions, preserving failure context.

    If original_error is provided and any cleanup fails, raises CleanupError
    containing both the original error and cleanup errors.

    Args:
        cleanup_funcs: List of (name, callable) tuples
        original_error: Optional original test failure to preserve

    Raises:
        CleanupError: If original_error is set and cleanup fails
    """
    cleanup_errors: list[tuple[str, Exception]] = []

    for name, func in cleanup_funcs:
        try:
            func()
        except Exception as e:
            cleanup_errors.append((name, e))

    if original_error is not None and cleanup_errors:
        raise CleanupError(original_error, cleanup_errors) from original_error

    # If no original error but cleanup failed, just log (don't raise)
    # This allows tests to pass even if cleanup has issues


def cleanup_minio_object(
    minio_client: "Minio",
    bucket: str,
    object_key: str,
) -> None:
    """Remove an object from MinIO (best-effort, no raise on error)."""
    try:
        minio_client.remove_object(bucket, object_key)
    except Exception:
        pass


def cleanup_mongodb_asset(
    mongo_client: "MongoClient[JsonDict]",
    mongo_settings: "MongoSettings",
    asset_doc: JsonDict | None,
) -> None:
    """Remove asset document from MongoDB (best-effort)."""
    if asset_doc is None:
        return
    try:
        db = mongo_client[mongo_settings.database]
        db["assets"].delete_one({"_id": asset_doc["_id"]})
    except Exception:
        pass


def cleanup_mongodb_run(
    mongo_client: "MongoClient[JsonDict]",
    mongo_settings: "MongoSettings",
    dagster_run_id: str,
) -> None:
    """Remove run document from MongoDB (best-effort)."""
    try:
        db = mongo_client[mongo_settings.database]
        db["runs"].delete_one({"dagster_run_id": dagster_run_id})
    except Exception:
        pass


def cleanup_mongodb_lineage(
    mongo_client: "MongoClient[JsonDict]",
    mongo_settings: "MongoSettings",
    asset_ids: list[str],
) -> None:
    """Remove lineage edges for given asset IDs (best-effort)."""
    if not asset_ids:
        return
    try:
        db = mongo_client[mongo_settings.database]
        db["lineage"].delete_many(
            {
                "$or": [
                    {"source_asset_id": {"$in": asset_ids}},
                    {"target_asset_id": {"$in": asset_ids}},
                ]
            }
        )
    except Exception:
        pass


def cleanup_dynamic_partitions(
    client: DagsterGraphQLClient,
    partition_keys: set[str],
    *,
    original_error: BaseException | None = None,
) -> None:
    """Delete dynamic partitions created during a test, preserving failure context.

    Only deletes partition keys from the provided set (created by this test).

    Args:
        client: DagsterGraphQLClient instance
        partition_keys: Set of partition keys to delete (created by this test)
        original_error: If set, cleanup errors combined with this into CleanupError
    """
    cleanup_errors: list[tuple[str, Exception]] = []

    for partition_key in partition_keys:
        try:
            delete_dynamic_partition(client, partition_key, strict=True)
        except Exception as e:
            cleanup_errors.append((f"partition:{partition_key}", e))

    if original_error is not None and cleanup_errors:
        raise CleanupError(original_error, cleanup_errors) from original_error
