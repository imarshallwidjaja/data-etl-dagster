"""
Migration 001: Baseline Schema (Consolidated)

This migration establishes the complete MongoDB schema for the ETL platform.
Consolidates original migrations 001-004 into single baseline.

History (for reference):
- Original 001: Basic collections and validators
- Original 002: Tabular support (already in 001)
- Original 003: Runs collection, unified status values
- Original 004: Enhanced metadata, column_schema, geometry_type

Schema constants are FROZEN - do not modify. Create new migration for changes.
"""

from pymongo.database import Database
from pymongo.errors import CollectionInvalid

VERSION = "001"

# =============================================================================
# FROZEN SCHEMA CONSTANTS - DO NOT MODIFY
# Generated from Pydantic models on 2025-12-23
# To update: create new migration with new schema version
# =============================================================================

ASSETS_SCHEMA_V001 = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "s3_key",
            "dataset_id",
            "version",
            "content_hash",
            "run_id",
            "kind",
            "format",
            "created_at",
            "metadata",
        ],
        "properties": {
            "s3_key": {"bsonType": "string"},
            "dataset_id": {"bsonType": "string"},
            "version": {"bsonType": "int", "minimum": 1},
            "content_hash": {
                "bsonType": "string",
                "pattern": "^sha256:[a-f0-9]{64}$",
            },
            "run_id": {"bsonType": "string"},
            "kind": {"enum": ["spatial", "tabular", "joined"]},
            "format": {"enum": ["geoparquet", "cog", "geojson", "parquet"]},
            "crs": {"bsonType": ["string", "null"]},
            "bounds": {
                "bsonType": ["object", "null"],
                "properties": {
                    "minx": {"bsonType": "double"},
                    "miny": {"bsonType": "double"},
                    "maxx": {"bsonType": "double"},
                    "maxy": {"bsonType": "double"},
                },
            },
            "metadata": {
                "bsonType": "object",
                "required": [
                    "title",
                    "description",
                    "keywords",
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
                    "tags": {
                        "bsonType": "object",
                        "additionalProperties": {
                            "bsonType": ["string", "int", "long", "double", "bool"]
                        },
                    },
                    "header_mapping": {
                        "bsonType": ["object", "null"],
                        "additionalProperties": {"bsonType": "string"},
                    },
                    "column_schema": {"bsonType": ["object", "null"]},
                    "geometry_type": {"bsonType": ["string", "null"]},
                },
            },
            "created_at": {"bsonType": "date"},
            "updated_at": {"bsonType": ["date", "null"]},
        },
    }
}

MANIFESTS_SCHEMA_V001 = {
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

RUNS_SCHEMA_V001 = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["dagster_run_id", "batch_id", "job_name", "status", "started_at"],
        "properties": {
            "dagster_run_id": {"bsonType": "string"},
            "batch_id": {"bsonType": "string"},
            "job_name": {"bsonType": "string"},
            "partition_key": {"bsonType": ["string", "null"]},
            "status": {"enum": ["running", "success", "failure", "canceled"]},
            "asset_ids": {"bsonType": "array", "items": {"bsonType": "objectId"}},
            "error_message": {"bsonType": ["string", "null"]},
            "started_at": {"bsonType": "date"},
            "completed_at": {"bsonType": ["date", "null"]},
        },
    }
}

LINEAGE_SCHEMA_V001 = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "source_asset_id",
            "target_asset_id",
            "run_id",
            "transformation",
            "created_at",
        ],
        "properties": {
            "source_asset_id": {"bsonType": "objectId"},
            "target_asset_id": {"bsonType": "objectId"},
            "run_id": {"bsonType": "objectId"},
            "transformation": {"bsonType": "string"},
            "parameters": {"bsonType": "object"},
            "created_at": {"bsonType": "date"},
        },
    }
}


def up(db: Database) -> None:
    """Apply baseline schema migration."""

    # Assets collection
    try:
        db.create_collection(
            "assets",
            validator=ASSETS_SCHEMA_V001,
            validationLevel="strict",
            validationAction="error",
        )
    except CollectionInvalid:
        db.command(
            "collMod",
            "assets",
            validator=ASSETS_SCHEMA_V001,
            validationLevel="strict",
            validationAction="error",
        )

    db.assets.create_index([("s3_key", 1)], unique=True)
    db.assets.create_index([("dataset_id", 1), ("version", -1)])
    db.assets.create_index([("content_hash", 1)])
    db.assets.create_index([("run_id", 1)])
    db.assets.create_index([("kind", 1)])
    db.assets.create_index([("created_at", -1)])
    db.assets.create_index([("metadata.keywords", 1)])
    db.assets.create_index([("kind", 1), ("dataset_id", 1)])

    # Manifests collection
    try:
        db.create_collection(
            "manifests",
            validator=MANIFESTS_SCHEMA_V001,
            validationLevel="strict",
            validationAction="error",
        )
    except CollectionInvalid:
        db.command(
            "collMod",
            "manifests",
            validator=MANIFESTS_SCHEMA_V001,
            validationLevel="strict",
            validationAction="error",
        )

    db.manifests.create_index([("batch_id", 1)], unique=True)
    db.manifests.create_index([("status", 1)])
    db.manifests.create_index([("ingested_at", -1)])

    # Runs collection
    try:
        db.create_collection(
            "runs",
            validator=RUNS_SCHEMA_V001,
            validationLevel="strict",
            validationAction="error",
        )
    except CollectionInvalid:
        db.command(
            "collMod",
            "runs",
            validator=RUNS_SCHEMA_V001,
            validationLevel="strict",
            validationAction="error",
        )

    db.runs.create_index([("dagster_run_id", 1)], unique=True, sparse=True)
    db.runs.create_index([("batch_id", 1)])
    db.runs.create_index([("status", 1)])
    db.runs.create_index([("batch_id", 1), ("started_at", -1)])

    # Lineage collection
    try:
        db.create_collection(
            "lineage",
            validator=LINEAGE_SCHEMA_V001,
            validationLevel="strict",
            validationAction="error",
        )
    except CollectionInvalid:
        db.command(
            "collMod",
            "lineage",
            validator=LINEAGE_SCHEMA_V001,
            validationLevel="strict",
            validationAction="error",
        )

    db.lineage.create_index([("source_asset_id", 1)])
    db.lineage.create_index([("target_asset_id", 1)])
    db.lineage.create_index([("run_id", 1)])


def down(db: Database) -> None:
    """
    Rollback migration (best effort).

    Note: This is destructive - drops all collections.
    Only use in development when resetting to clean state.
    """
    for collection_name in ["assets", "manifests", "runs", "lineage"]:
        if collection_name in db.list_collection_names():
            db.drop_collection(collection_name)
