"""
Migration 003: Activity Logs Collection

Creates the activity_logs collection for audit trail and user activity tracking.

Indexes:
- timestamp descending (for recent activity queries)
- user + timestamp descending (for user activity history)
- action + timestamp descending (for action-type queries)
- resource_type + resource_id (for resource-specific lookups)
"""

from pymongo.database import Database
from pymongo import DESCENDING

VERSION = "003"

ACTIVITY_LOGS_SCHEMA_V003 = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["timestamp", "user", "action", "resource_type", "resource_id"],
        "properties": {
            "timestamp": {"bsonType": "date"},
            "user": {"bsonType": "string"},
            "action": {
                "enum": [
                    "create_manifest",
                    "rerun_manifest",
                    "delete_manifest",
                    "upload_file",
                    "delete_file",
                    "download_asset",
                    "run_started",
                    "run_success",
                    "run_failure",
                    "run_canceled",
                ]
            },
            "resource_type": {"enum": ["manifest", "file", "asset", "run"]},
            "resource_id": {"bsonType": "string"},
            "details": {"bsonType": "object"},
        },
    }
}


def up(db: Database) -> None:
    """Create activity_logs collection with indexes."""
    collection_name = "activity_logs"

    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name, validator=ACTIVITY_LOGS_SCHEMA_V003)
    else:
        db.command("collMod", collection_name, validator=ACTIVITY_LOGS_SCHEMA_V003)

    existing = {idx["name"] for idx in db[collection_name].list_indexes()}

    if "timestamp_desc" not in existing:
        db[collection_name].create_index(
            [("timestamp", DESCENDING)],
            name="timestamp_desc",
        )

    if "user_timestamp_desc" not in existing:
        db[collection_name].create_index(
            [("user", 1), ("timestamp", DESCENDING)],
            name="user_timestamp_desc",
        )

    if "action_timestamp_desc" not in existing:
        db[collection_name].create_index(
            [("action", 1), ("timestamp", DESCENDING)],
            name="action_timestamp_desc",
        )

    if "resource_type_resource_id" not in existing:
        db[collection_name].create_index(
            [("resource_type", 1), ("resource_id", 1)],
            name="resource_type_resource_id",
        )


def down(db: Database) -> None:
    """Drop activity_logs collection."""
    if "activity_logs" in db.list_collection_names():
        db.drop_collection("activity_logs")
