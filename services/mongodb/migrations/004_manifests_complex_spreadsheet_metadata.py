"""
Migration 004: Add manifests.metadata.complex_spreadsheet schema

Adds support for complex spreadsheet processing metadata in manifest records.
"""

from pymongo.database import Database

VERSION = "004"


MANIFESTS_SCHEMA_V004 = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "batch_id",
            "uploader",
            "intent",
            "files",
            "metadata",
            "status",
            "ingested_at",
        ],
        "properties": {
            "batch_id": {"bsonType": "string"},
            "uploader": {"bsonType": "string"},
            "intent": {"bsonType": "string"},
            "files": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["path", "type", "format"],
                    "properties": {
                        "path": {"bsonType": "string"},
                        "type": {"enum": ["raster", "vector", "tabular"]},
                        "format": {"bsonType": "string"},
                    },
                    "additionalProperties": False,
                },
            },
            "metadata": {
                "bsonType": "object",
                "required": [
                    "title",
                    "description",
                    "source",
                    "license",
                    "attribution",
                ],
                "properties": {
                    "title": {"bsonType": "string"},
                    "description": {"bsonType": "string"},
                    "keywords": {"bsonType": "array", "items": {"bsonType": "string"}},
                    "source": {"bsonType": "string"},
                    "license": {"bsonType": "string"},
                    "attribution": {"bsonType": "string"},
                    "project": {"bsonType": ["string", "null"]},
                    "tags": {
                        "bsonType": ["object", "null"],
                        "additionalProperties": {
                            "bsonType": ["string", "int", "long", "double", "bool"]
                        },
                    },
                    "join_config": {"bsonType": ["object", "null"]},
                    "complex_spreadsheet": {
                        "bsonType": ["object", "null"],
                        "required": ["template_id", "template_params"],
                        "properties": {
                            "template_id": {"bsonType": "string"},
                            "template_params": {"bsonType": "object"},
                        },
                    },
                },
            },
            "status": {"enum": ["running", "success", "failure", "canceled"]},
            "error_message": {"bsonType": ["string", "null"]},
            "ingested_at": {"bsonType": "date"},
            "completed_at": {"bsonType": ["date", "null"]},
            "updated_at": {"bsonType": ["date", "null"]},
        },
    }
}


def up(db: Database) -> None:
    """Update manifests validator to schema v004."""
    db.command("collMod", "manifests", validator=MANIFESTS_SCHEMA_V004)


def down(db: Database) -> None:
    """No-op rollback for forward-only schema migration."""
