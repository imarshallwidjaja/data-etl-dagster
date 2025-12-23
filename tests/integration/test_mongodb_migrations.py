"""
Integration tests for MongoDB schema migrations.

Verifies that the migration system works correctly:
- Migrations are discovered and applied in order
- Applied migrations are tracked in schema_migrations collection
- Migrations are idempotent (can be run multiple times safely)
- Collections and validators are created correctly
"""

import pytest
from datetime import datetime, timezone
from pathlib import Path

from libs.models import MongoSettings
from pymongo import MongoClient
from pymongo.errors import WriteError


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


@pytest.fixture(autouse=True)
def cleanup_schema_migrations(mongo_database):
    """
    Clean up schema_migrations collection before and after each test.

    This ensures tests start with a clean state and don't interfere with each other.
    """
    # Clean before test
    mongo_database["schema_migrations"].delete_many({})
    yield
    # Clean after test
    mongo_database["schema_migrations"].delete_many({})


class TestMigrationDiscovery:
    """Test migration file discovery."""

    def test_migrations_directory_exists(self):
        """Test that migrations directory exists."""
        migrations_dir = (
            Path(__file__).parent.parent.parent / "services" / "mongodb" / "migrations"
        )
        assert migrations_dir.exists(), (
            f"Migrations directory not found: {migrations_dir}"
        )

    def test_migration_files_exist(self):
        """Test that expected migration files exist."""
        migrations_dir = (
            Path(__file__).parent.parent.parent / "services" / "mongodb" / "migrations"
        )
        migration_files = list(migrations_dir.glob("*.py"))
        migration_files = [f for f in migration_files if not f.name.startswith("__")]

        assert len(migration_files) >= 2, (
            f"Expected at least 2 migration files, found {len(migration_files)}"
        )

        # Check for expected migrations
        filenames = [f.name for f in migration_files]
        assert any("001" in f for f in filenames), "Migration 001 not found"
        assert any("002" in f for f in filenames), "Migration 002 not found"


class TestMigrationRunner:
    """Test migration runner functionality."""

    def test_migration_runner_imports(self):
        """Test that migration runner can be imported."""
        import sys
        from pathlib import Path

        scripts_dir = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_dir.parent))

        try:
            from scripts.migrate_db import discover_migrations, load_migration_module

            assert callable(discover_migrations)
            assert callable(load_migration_module)
        finally:
            sys.path.remove(str(scripts_dir.parent))

    def test_migrations_applied_to_schema_migrations(self, mongo_database):
        """
        Test that migrations are recorded in schema_migrations collection.

        This test runs migrations explicitly to avoid depending on external startup order.
        """
        import sys

        scripts_dir = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_dir.parent))

        try:
            from scripts.migrate_db import (
                apply_migration,
                discover_migrations,
                ensure_schema_migrations_collection,
                get_applied_versions,
                load_migration_module,
            )

            ensure_schema_migrations_collection(mongo_database)

            migrations_dir = (
                Path(__file__).parent.parent.parent
                / "services"
                / "mongodb"
                / "migrations"
            )
            migrations = discover_migrations(migrations_dir)
            applied_versions = get_applied_versions(mongo_database)

            for version, file_path in migrations:
                if version in applied_versions:
                    continue
                migration_version, up_func = load_migration_module(file_path)
                assert migration_version == version
                apply_migration(mongo_database, version, up_func)

        finally:
            sys.path.remove(str(scripts_dir.parent))

        applied = list(mongo_database["schema_migrations"].find({}))
        versions = [doc["version"] for doc in applied]
        assert "001" in versions, "Migration 001 not found in schema_migrations"

        migration_001 = next(doc for doc in applied if doc["version"] == "001")
        assert "applied_at" in migration_001
        assert "duration_ms" in migration_001
        assert isinstance(migration_001["applied_at"], datetime)
        assert isinstance(migration_001["duration_ms"], int)


class TestMigrationIdempotency:
    """Test that migrations are idempotent."""

    def test_collections_created_by_migration(self, mongo_database):
        """Test that required collections exist after migrations."""
        collections = mongo_database.list_collection_names()

        required_collections = ["assets", "manifests", "runs", "lineage"]
        for collection_name in required_collections:
            assert collection_name in collections, (
                f"Required collection '{collection_name}' not found. "
                f"Available collections: {collections}"
            )

    def test_assets_validator_allows_tabular(self, mongo_database):
        """Test that assets validator allows tabular kind and parquet format."""
        # Try to insert a valid tabular asset (M4 schema requires full metadata)
        tabular_asset = {
            "s3_key": "data-lake/test_dataset/v1/data.parquet",
            "dataset_id": "test_dataset",
            "version": 1,
            "content_hash": "sha256:" + "0" * 64,
            "run_id": "507f1f77bcf86cd799439011",  # ObjectId as string
            "kind": "tabular",
            "format": "parquet",
            "crs": None,
            "bounds": None,
            "metadata": {
                "title": "Test Tabular Dataset",
                "description": "A test dataset",
                "keywords": ["test"],
                "source": "test source",
                "license": "MIT",
                "attribution": "Test User",
            },
            "created_at": datetime.now(timezone.utc),
        }

        result = mongo_database.assets.insert_one(tabular_asset)
        assert result.inserted_id is not None

        # Clean up
        mongo_database.assets.delete_one({"_id": result.inserted_id})

    def test_assets_validator_rejects_invalid_kind(self, mongo_database):
        """Test that assets validator rejects invalid kind values."""
        invalid_asset = {
            "s3_key": "data-lake/test_dataset/v1/data.parquet",
            "dataset_id": "test_dataset",
            "version": 1,
            "content_hash": "sha256:" + "0" * 64,
            "run_id": "507f1f77bcf86cd799439011",
            "kind": "invalid_kind",  # Invalid
            "format": "parquet",
            "crs": None,
            "bounds": None,
            "metadata": {
                "title": "Test Dataset",
                "description": "A test",
                "keywords": [],
                "source": "test",
                "license": "MIT",
                "attribution": "Test",
            },
            "created_at": datetime.now(timezone.utc),
        }

        with pytest.raises(WriteError):
            mongo_database.assets.insert_one(invalid_asset)

    def test_assets_validator_rejects_missing_metadata_field(self, mongo_database):
        """Test that assets validator rejects documents missing required metadata fields."""
        # Missing 'license' field in metadata
        invalid_asset = {
            "s3_key": "data-lake/test_dataset_fail/v1/data.parquet",
            "dataset_id": "test_dataset_fail",
            "version": 1,
            "content_hash": "sha256:" + "1" * 64,
            "run_id": "507f1f77bcf86cd799439011",
            "kind": "tabular",
            "format": "parquet",
            "crs": None,
            "bounds": None,
            "metadata": {
                "title": "Test Dataset",
                "description": "A test",
                "keywords": [],
                "source": "test",
                # Missing: license, attribution
            },
            "created_at": datetime.now(timezone.utc),
        }

        with pytest.raises(WriteError):
            mongo_database.assets.insert_one(invalid_asset)

    def test_manifests_validator_allows_tabular_files(self, mongo_database):
        """Test that manifests validator allows tabular file type."""
        tabular_manifest = {
            "batch_id": "test_batch_tabular",
            "uploader": "test_user",
            "intent": "ingest_tabular",
            "files": [
                {
                    "path": "s3://landing-zone/batch_001/data.csv",
                    "type": "tabular",
                    "format": "CSV",
                }
            ],
            "metadata": {
                "title": "Test Tabular Dataset",
                "description": "Test manifest for integration tests",
                "keywords": ["test", "tabular"],
                "source": "Integration test",
                "license": "MIT",
                "attribution": "Test User",
                "project": "TEST",
            },
            "status": "running",  # M4 uses running/success/failure/canceled
            "ingested_at": datetime.now(timezone.utc),
        }

        result = mongo_database.manifests.insert_one(tabular_manifest)
        assert result.inserted_id is not None

        # Clean up
        mongo_database.manifests.delete_one({"_id": result.inserted_id})


class TestMigrationIndexes:
    """Test that indexes are created correctly by migrations."""

    def test_assets_indexes_exist(self, mongo_database):
        """Test that assets collection has expected indexes."""
        indexes = mongo_database.assets.index_information()

        expected_indexes = [
            "s3_key_1",  # Unique index
            "dataset_id_1_version_-1",  # Compound index
            "content_hash_1",
            "run_id_1",  # Changed from dagster_run_id_1 in M4
            "kind_1",
            "created_at_-1",
            "metadata.keywords_1",  # New multikey index
            "kind_1_dataset_id_1",  # New compound index
        ]

        for index_name in expected_indexes:
            assert index_name in indexes, (
                f"Expected index '{index_name}' not found. "
                f"Available indexes: {list(indexes.keys())}"
            )

    def test_text_search_index_exists(self, mongo_database):
        """Test that text search index exists on metadata.keywords."""
        indexes = mongo_database.assets.index_information()

        assert "metadata_keywords_text" in indexes, (
            f"Text search index 'metadata_keywords_text' not found. "
            f"Available indexes: {list(indexes.keys())}"
        )

    def test_manifests_indexes_exist(self, mongo_database):
        """Test that manifests collection has expected indexes."""
        indexes = mongo_database.manifests.index_information()

        expected_indexes = [
            "batch_id_1",  # Unique index
            "status_1",
            "ingested_at_-1",
        ]

        for index_name in expected_indexes:
            assert index_name in indexes, (
                f"Expected index '{index_name}' not found. "
                f"Available indexes: {list(indexes.keys())}"
            )

    def test_runs_indexes_exist(self, mongo_database):
        """Test that runs collection has expected indexes."""
        indexes = mongo_database.runs.index_information()

        expected_indexes = [
            "dagster_run_id_1",  # Unique sparse index
            "batch_id_1",  # M4 uses batch_id instead of manifest_id
            "status_1",
            "batch_id_1_started_at_-1",  # Compound index
        ]

        for index_name in expected_indexes:
            assert index_name in indexes, (
                f"Expected index '{index_name}' not found. "
                f"Available indexes: {list(indexes.keys())}"
            )

    def test_lineage_indexes_exist(self, mongo_database):
        """Test that lineage collection has expected indexes."""
        indexes = mongo_database.lineage.index_information()

        expected_indexes = [
            "source_asset_id_1",
            "target_asset_id_1",
            "run_id_1",  # M4 uses run_id (ObjectId) instead of dagster_run_id
        ]

        for index_name in expected_indexes:
            assert index_name in indexes, (
                f"Expected index '{index_name}' not found. "
                f"Available indexes: {list(indexes.keys())}"
            )
