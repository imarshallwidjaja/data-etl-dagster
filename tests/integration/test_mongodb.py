"""
Integration tests for MongoDB (metadata ledger).

Tests basic connectivity, database access, and CRUD operations.
"""

import pytest
from datetime import datetime

from libs.models import MongoSettings
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError


pytestmark = pytest.mark.integration


@pytest.fixture
def mongo_settings():
    """Load MongoDB settings from environment."""
    return MongoSettings()


@pytest.fixture
def mongo_client(mongo_settings):
    """Create MongoDB client."""
    return MongoClient(
        mongo_settings.connection_string,
        serverSelectionTimeoutMS=5000,
    )


@pytest.fixture
def test_collection(mongo_client, mongo_settings):
    """Get test collection and clean up after test."""
    db = mongo_client[mongo_settings.database]
    collection = db["integration_test"]
    
    # Cleanup before test
    collection.delete_many({})
    
    yield collection
    
    # Cleanup after test
    collection.delete_many({})


class TestMongoDBConnectivity:
    """Test MongoDB connectivity and basic operations."""
    
    def test_mongodb_connection(self, mongo_client):
        """Test that MongoDB is accessible."""
        # Ping the server
        result = mongo_client.admin.command("ping")
        assert result.get("ok") == 1.0
    
    def test_database_access(self, mongo_client, mongo_settings):
        """Test that we can access the configured database."""
        db = mongo_client[mongo_settings.database]
        # List collections to verify database access
        collections = db.list_collection_names()
        assert isinstance(collections, list)
    
    def test_mongodb_crud_operations(self, test_collection):
        """Test basic CRUD operations."""
        # Create
        test_doc = {
            "test_id": "integration_test_001",
            "message": "Hello, MongoDB!",
            "timestamp": datetime.now(),
            "metadata": {
                "source": "integration_test",
                "version": 1,
            },
        }
        result = test_collection.insert_one(test_doc)
        assert result.inserted_id is not None
        
        # Read
        found_doc = test_collection.find_one({"test_id": "integration_test_001"})
        assert found_doc is not None
        assert found_doc["message"] == "Hello, MongoDB!"
        assert found_doc["metadata"]["source"] == "integration_test"
        
        # Update
        test_collection.update_one(
            {"test_id": "integration_test_001"},
            {"$set": {"message": "Updated message"}},
        )
        updated_doc = test_collection.find_one({"test_id": "integration_test_001"})
        assert updated_doc["message"] == "Updated message"
        
        # Delete
        delete_result = test_collection.delete_one({"test_id": "integration_test_001"})
        assert delete_result.deleted_count == 1
        
        # Verify deletion
        deleted_doc = test_collection.find_one({"test_id": "integration_test_001"})
        assert deleted_doc is None

