"""
Migration 004: Add complex_spreadsheet to manifests metadata

Adds metadata.complex_spreadsheet field to the manifests collection schema.
This supports complex spreadsheet ingestion with template-driven splitting.

Schema change:
- metadata.properties.complex_spreadsheet: object|null
  - template_id: string (required)
  - template_params: object (required)
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
                    "keywords": {
                        "bsonType": "array",
                        "items": {"bsonType": "string"},
                    },
                    "source": {"bsonType": "string"},
                    "license": {"bsonType": "string"},
                    "attribution": {"bsonType": "string"},
                    "project": {"bsonType": ["string", "null"]},
                    "tags": {
                        "bsonType": ["object", "null"],
                        "additionalProperties": {
                            "bsonType": [
                                "string",
                                "int",
                                "long",
                                "double",
                                "bool",
                            ]
                        },
                    },
                    "join_config": {"bsonType": ["object", "null"]},
                    "complex_spreadsheet": {
                        "bsonType": ["object", "null"],
                        "properties": {
                            "template_id": {"bsonType": "string"},
                            "template_params": {"bsonType": "object"},
                        },
                        "required": ["template_id", "template_params"],
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
    """Update manifests collection validator to include complex_spreadsheet."""
    db.command("collMod", "manifests", validator=MANIFESTS_SCHEMA_V004)


def down(db: Database) -> None:
    """Revert manifests collection validator to v001 baseline.

    Uses importlib because 001_baseline_schema.py has a digit-prefixed name.
    """
    import importlib.util
    from pathlib import Path

    baseline_path = Path(__file__).parent / "001_baseline_schema.py"
    spec = importlib.util.spec_from_file_location("baseline", baseline_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load baseline migration from {baseline_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    db.command("collMod", "manifests", validator=module.MANIFESTS_SCHEMA_V001)
