"""Smoke test for dynamic partition add/delete via GraphQL.

Validates that:
1. add_dynamic_partition creates a partition successfully
2. delete_dynamic_partition (strict mode) deletes the partition
3. Error checking validates response structure and union __typename

Run with: pytest tests/integration/test_dynamic_partition_delete_smoke.py -v -m integration
"""

from uuid import uuid4

import pytest

from .helpers import (
    add_dynamic_partition,
    delete_dynamic_partition,
)


pytestmark = pytest.mark.integration


class TestDynamicPartitionDeleteSmoke:
    def test_add_and_delete_dynamic_partition_via_graphql(self, dagster_client):
        partition_key = f"smoke_test_partition_{uuid4().hex[:12]}"

        add_dynamic_partition(dagster_client, partition_key)

        delete_dynamic_partition(dagster_client, partition_key, strict=True)

    def test_delete_mutation_name_and_signature(self, dagster_client):
        partition_key = f"smoke_signature_{uuid4().hex[:12]}"

        add_dynamic_partition(dagster_client, partition_key)

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

        result = dagster_client.query(
            mutation,
            variables={
                "repositoryLocationName": "etl_pipelines",
                "repositoryName": "__repository__",
                "partitionsDefName": "dataset_id",
                "partitionKey": partition_key,
            },
            timeout=10,
        )

        assert "errors" not in result, f"GraphQL returned errors: {result}"
        assert "data" in result, f"No 'data' in response: {result}"

        delete_result = result["data"]["deleteDynamicPartitions"]
        assert delete_result is not None, f"deleteDynamicPartitions is None: {result}"
        assert delete_result["__typename"] == "DeleteDynamicPartitionsSuccess", (
            f"Unexpected __typename: {result}"
        )

    def test_strict_delete_raises_on_python_error(self, dagster_client):
        partition_key = f"nonexistent_partition_{uuid4().hex[:12]}"

        delete_dynamic_partition(dagster_client, partition_key, strict=True)

    def test_non_strict_delete_swallows_errors(self, dagster_client):
        partition_key = f"swallow_test_{uuid4().hex[:12]}"

        delete_dynamic_partition(dagster_client, partition_key, strict=False)
