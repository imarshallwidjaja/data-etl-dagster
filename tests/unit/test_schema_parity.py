"""
Schema parity tests - verify Pydantic models match latest migration schemas.

These tests FAIL when Pydantic models change without a corresponding migration.
This prevents drift between Python validation and MongoDB validation.

NOTE: Migration files start with digits (e.g., 001_baseline_schema.py) which
can't be imported directly. We use importlib to load them dynamically.
"""

import importlib.util
from pathlib import Path


def load_migration_schema(migration_filename: str):
    """Load a migration module using importlib (handles digit-prefixed names)."""
    migrations_dir = (
        Path(__file__).parent.parent.parent / "services" / "mongodb" / "migrations"
    )
    file_path = migrations_dir / migration_filename

    spec = importlib.util.spec_from_file_location("migration", file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load migration from {file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Load schemas once at module level
_baseline = load_migration_schema("001_baseline_schema.py")
ASSETS_SCHEMA_V001 = _baseline.ASSETS_SCHEMA_V001
MANIFESTS_SCHEMA_V001 = _baseline.MANIFESTS_SCHEMA_V001
RUNS_SCHEMA_V001 = _baseline.RUNS_SCHEMA_V001
LINEAGE_SCHEMA_V001 = _baseline.LINEAGE_SCHEMA_V001


class TestAssetSchemaParity:
    """Verify Asset model matches latest migration schema."""

    def test_asset_required_fields_match(self):
        """Test that required fields in Pydantic match migration schema."""
        from libs.models import Asset

        pydantic_required = {
            f for f, info in Asset.model_fields.items() if info.is_required()
        }
        mongo_required = set(ASSETS_SCHEMA_V001["$jsonSchema"].get("required", []))

        # These fields can be optional (have defaults in Pydantic or can be null)
        skippable_fields = {"bounds", "crs", "updated_at"}
        missing_in_mongo = pydantic_required - mongo_required - skippable_fields

        assert not missing_in_mongo, (
            f"Pydantic requires fields not in MongoDB schema: {missing_in_mongo}. "
            "If intentional, create a new migration."
        )

    def test_asset_kind_enum_matches(self):
        """Test that kind enum values match between Pydantic and MongoDB."""
        from libs.models import AssetKind

        pydantic_values = {k.value for k in AssetKind}
        mongo_values = set(
            ASSETS_SCHEMA_V001["$jsonSchema"]["properties"]["kind"]["enum"]
        )

        assert pydantic_values == mongo_values, (
            f"Kind enum mismatch. Pydantic: {pydantic_values}, MongoDB: {mongo_values}"
        )

    def test_asset_format_enum_matches(self):
        """Test that format enum values match between Pydantic and MongoDB."""
        from libs.models import OutputFormat

        pydantic_values = {f.value for f in OutputFormat}
        mongo_values = set(
            ASSETS_SCHEMA_V001["$jsonSchema"]["properties"]["format"]["enum"]
        )

        assert pydantic_values == mongo_values, (
            f"Format enum mismatch. Pydantic: {pydantic_values}, MongoDB: {mongo_values}"
        )

    def test_asset_created_at_is_date(self):
        """Test that created_at field is validated as bsonType: date."""
        created_at_schema = ASSETS_SCHEMA_V001["$jsonSchema"]["properties"][
            "created_at"
        ]
        assert created_at_schema.get("bsonType") == "date", (
            f"Expected created_at bsonType 'date', got {created_at_schema}"
        )

    def test_asset_version_is_int(self):
        """Test that version field is validated as bsonType: int."""
        version_schema = ASSETS_SCHEMA_V001["$jsonSchema"]["properties"]["version"]
        assert version_schema.get("bsonType") == "int", (
            f"Expected version bsonType 'int', got {version_schema}"
        )


class TestManifestSchemaParity:
    """Verify ManifestRecord matches latest migration schema."""

    def test_manifest_status_enum_matches(self):
        """Test that status enum values match."""
        from libs.models import ManifestStatus

        pydantic_values = {s.value for s in ManifestStatus}
        mongo_values = set(
            MANIFESTS_SCHEMA_V001["$jsonSchema"]["properties"]["status"]["enum"]
        )

        assert pydantic_values == mongo_values, (
            f"Status enum mismatch. Pydantic: {pydantic_values}, MongoDB: {mongo_values}"
        )


class TestRunSchemaParity:
    """Verify Run model matches latest migration schema."""

    def test_run_status_enum_matches(self):
        """Test that run status enum values match."""
        from libs.models import RunStatus

        pydantic_values = {s.value for s in RunStatus}
        mongo_values = set(
            RUNS_SCHEMA_V001["$jsonSchema"]["properties"]["status"]["enum"]
        )

        assert pydantic_values == mongo_values, (
            f"Status enum mismatch. Pydantic: {pydantic_values}, MongoDB: {mongo_values}"
        )


class TestSchemaConstants:
    """Test that schema constants are properly structured."""

    def test_assets_schema_has_jsonschema(self):
        """Verify ASSETS_SCHEMA_V001 has $jsonSchema wrapper."""
        assert "$jsonSchema" in ASSETS_SCHEMA_V001

    def test_manifests_schema_has_jsonschema(self):
        """Verify MANIFESTS_SCHEMA_V001 has $jsonSchema wrapper."""
        assert "$jsonSchema" in MANIFESTS_SCHEMA_V001

    def test_runs_schema_has_jsonschema(self):
        """Verify RUNS_SCHEMA_V001 has $jsonSchema wrapper."""
        assert "$jsonSchema" in RUNS_SCHEMA_V001

    def test_lineage_schema_has_jsonschema(self):
        """Verify LINEAGE_SCHEMA_V001 has $jsonSchema wrapper."""
        assert "$jsonSchema" in LINEAGE_SCHEMA_V001
