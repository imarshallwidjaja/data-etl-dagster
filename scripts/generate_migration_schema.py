#!/usr/bin/env python
"""
Generate MongoDB schema from Pydantic model.

Usage:
    python scripts/generate_migration_schema.py Asset
    python scripts/generate_migration_schema.py ManifestRecord
    python scripts/generate_migration_schema.py Run

Output is printed to stdout - copy into migration as frozen constant.
This script is for write-time convenience only, never called at runtime.
"""

import sys
import json
from typing import Any


def json_type_to_bson(json_type: str | list) -> str | list:
    """Map JSON Schema types to MongoDB bsonTypes."""
    mapping = {
        "string": "string",
        "integer": "int",
        "number": "double",
        "boolean": "bool",
        "array": "array",
        "object": "object",
        "null": "null",
    }
    if isinstance(json_type, list):
        return [mapping.get(t, t) for t in json_type]
    return mapping.get(json_type, json_type)


def transform_schema(schema: dict, defs: dict | None = None) -> dict:
    """Recursively transform JSON Schema to MongoDB bsonType format."""
    import warnings

    if defs is None:
        defs = schema.get("$defs", {})

    # Handle $ref
    if "$ref" in schema:
        ref_name = schema["$ref"].split("/")[-1]
        if ref_name in defs:
            return transform_schema(defs[ref_name], defs)
        return {"bsonType": "object"}

    result: dict[str, Any] = {}

    # Handle anyOf (Optional types)
    if "anyOf" in schema:
        types = []
        for option in schema["anyOf"]:
            if option.get("type") == "null":
                types.append("null")
            elif "$ref" in option:
                types.append("object")
            elif "type" in option:
                types.append(json_type_to_bson(option["type"]))
        if len(types) == 1:
            result["bsonType"] = types[0]
        else:
            result["bsonType"] = types
        return result

    # Handle format (date-time -> date)
    if schema.get("format") == "date-time":
        result["bsonType"] = "date"
        return result

    if "type" in schema:
        result["bsonType"] = json_type_to_bson(schema["type"])

    if "enum" in schema:
        result["enum"] = schema["enum"]

    if "const" in schema:
        # MongoDB doesn't have const, use enum with single value
        result["enum"] = [schema["const"]]

    if "pattern" in schema:
        result["pattern"] = schema["pattern"]

    if "minimum" in schema:
        result["minimum"] = schema["minimum"]

    if "properties" in schema:
        result["properties"] = {
            k: transform_schema(v, defs) for k, v in schema["properties"].items()
        }

    if "required" in schema:
        result["required"] = schema["required"]

    if "items" in schema:
        result["items"] = transform_schema(schema["items"], defs)

    if "additionalProperties" in schema:
        if isinstance(schema["additionalProperties"], bool):
            result["additionalProperties"] = schema["additionalProperties"]
        else:
            result["additionalProperties"] = transform_schema(
                schema["additionalProperties"], defs
            )

    # Warn about unhandled keywords
    handled_keywords = {
        "$ref",
        "anyOf",
        "type",
        "enum",
        "const",
        "pattern",
        "minimum",
        "properties",
        "required",
        "items",
        "additionalProperties",
        "format",
        "$defs",
        "title",
        "description",
        "default",
        "examples",
    }
    unhandled = set(schema.keys()) - handled_keywords
    if unhandled:
        warnings.warn(
            f"Unhandled JSON Schema keywords (may need manual mapping): {unhandled}",
            UserWarning,
            stacklevel=2,
        )

    return result


def pydantic_to_mongodb_schema(model_class) -> dict:
    """Convert Pydantic model to MongoDB $jsonSchema validator."""
    json_schema = model_class.model_json_schema()
    transformed = transform_schema(json_schema)
    return {"$jsonSchema": transformed}


def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_migration_schema.py <ModelName>")
        print("Available: Asset, ManifestRecord, Run")
        sys.exit(1)

    model_name = sys.argv[1]

    # Import models
    from libs.models import Asset, ManifestRecord, Run

    models = {
        "Asset": Asset,
        "ManifestRecord": ManifestRecord,
        "Run": Run,
    }

    if model_name not in models:
        print(f"Unknown model: {model_name}")
        print(f"Available: {', '.join(models.keys())}")
        sys.exit(1)

    schema = pydantic_to_mongodb_schema(models[model_name])

    # Pretty print for copying into migration
    print(f"{model_name.upper()}_SCHEMA = ", end="")
    print(json.dumps(schema, indent=4))


if __name__ == "__main__":
    main()
