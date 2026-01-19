"""
Integration tests for MongoDB initialization verification.

Verifies that MongoDB initialization scripts have run correctly by checking:
- Required collections exist (assets, manifests, runs, lineage)
- Expected indexes exist for critical collections
"""

import pytest

from libs.models import MongoSettings
from pymongo import MongoClient


pytestmark = pytest.mark.integration


@pytest.fixture
def mongo_settings():
    """Load MongoDB settings from environment."""
    return MongoSettings()


@pytest.fixture
def mongo_client(mongo_settings):
    """Create MongoDB client."""
    client = MongoClient(
        mongo_settings.connection_string,
        serverSelectionTimeoutMS=5000,
    )
    yield client
    client.close()


@pytest.fixture
def mongo_database(mongo_client, mongo_settings):
    """Get MongoDB database."""
    return mongo_client[mongo_settings.database]


class TestMongoDBInitialization:
    """Test MongoDB initialization invariants."""

    def test_required_collections_exist(self, mongo_database):
        """Test that all required collections exist."""
        collections = mongo_database.list_collection_names()

        required_collections = ["assets", "manifests", "runs", "lineage"]
        for collection_name in required_collections:
            assert collection_name in collections, (
                f"Required collection '{collection_name}' not found. "
                f"Available collections: {collections}"
            )

    def test_assets_collection_indexes(self, mongo_database):
        """Test that assets collection has expected indexes."""
        indexes = mongo_database.assets.index_information()

        expected_indexes = [
            "s3_key_1",
            "dataset_id_1_version_-1",
            "content_hash_1",
            "run_id_1",
            "created_at_-1",
        ]

        for index_name in expected_indexes:
            assert index_name in indexes, (
                f"Expected index '{index_name}' not found in assets collection. "
                f"Available indexes: {list(indexes.keys())}"
            )

    def test_manifests_collection_indexes(self, mongo_database):
        """Test that manifests collection has expected indexes."""
        indexes = mongo_database.manifests.index_information()

        # Check for critical indexes
        expected_indexes = [
            "batch_id_1",  # Unique index on batch_id
            "status_1",
            "ingested_at_-1",
        ]

        for index_name in expected_indexes:
            assert index_name in indexes, (
                f"Expected index '{index_name}' not found in manifests collection. "
                f"Available indexes: {list(indexes.keys())}"
            )

    def test_runs_collection_indexes(self, mongo_database):
        """Test that runs collection has expected indexes."""
        indexes = mongo_database.runs.index_information()

        expected_indexes = [
            "dagster_run_id_1",
            "batch_id_1",
            "status_1",
        ]

        for index_name in expected_indexes:
            assert index_name in indexes, (
                f"Expected index '{index_name}' not found in runs collection. "
                f"Available indexes: {list(indexes.keys())}"
            )

    def test_lineage_collection_indexes(self, mongo_database):
        """Test that lineage collection has expected indexes."""
        indexes = mongo_database.lineage.index_information()

        expected_indexes = [
            "source_asset_id_1",
            "target_asset_id_1",
            "run_id_1",
        ]

        for index_name in expected_indexes:
            assert index_name in indexes, (
                f"Expected index '{index_name}' not found in lineage collection. "
                f"Available indexes: {list(indexes.keys())}"
            )
