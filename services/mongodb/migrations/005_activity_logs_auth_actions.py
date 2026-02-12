"""
Migration 005: Add auth actions to activity_logs schema

Extends the activity_logs validator with authentication-related actions
(login_success, login_failure, logout, unauthorized_access), adds 'auth'
resource type, and adds optional ip_address field.

Schema changes from V003:
- action enum: +login_success, +login_failure, +logout, +unauthorized_access
- resource_type enum: +auth
- properties: +ip_address (bsonType: ["string", "null"])
"""

from pymongo.database import Database

VERSION = "005"

ACTIVITY_LOGS_SCHEMA_V005 = {
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
                    "archive_raw_source",
                    "run_started",
                    "run_success",
                    "run_failure",
                    "run_canceled",
                    "login_success",
                    "login_failure",
                    "logout",
                    "unauthorized_access",
                ]
            },
            "resource_type": {
                "enum": ["manifest", "file", "asset", "artifact", "run", "auth"]
            },
            "resource_id": {"bsonType": "string"},
            "details": {"bsonType": "object"},
            "ip_address": {"bsonType": ["string", "null"]},
        },
    }
}


def up(db: Database) -> None:
    """Update activity_logs collection validator with auth actions."""
    db.command("collMod", "activity_logs", validator=ACTIVITY_LOGS_SCHEMA_V005)


def down(db: Database) -> None:
    """Revert activity_logs validator to V003 schema."""
    import importlib.util
    from pathlib import Path

    v003_path = Path(__file__).parent / "003_activity_logs.py"
    spec = importlib.util.spec_from_file_location("migration_003", v003_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load migration from {v003_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    db.command("collMod", "activity_logs", validator=module.ACTIVITY_LOGS_SCHEMA_V003)
