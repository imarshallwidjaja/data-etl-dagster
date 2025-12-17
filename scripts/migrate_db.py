# =============================================================================
# MongoDB Schema Migration Runner
# =============================================================================
# Orchestrates MongoDB schema migrations in a version-controlled, idempotent
# manner. Discovers migration files in services/mongodb/migrations/ and
# applies them sequentially, tracking applied versions in schema_migrations.
# =============================================================================

import sys
import time
import importlib.util
from pathlib import Path
from typing import Callable
from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import CollectionInvalid, OperationFailure

from libs.models import MongoSettings


def discover_migrations(migrations_dir: Path) -> list[tuple[str, Path]]:
    """
    Discover migration files in the migrations directory.
    
    Migration files must match pattern: NNN_*.py where NNN is a zero-padded
    3-digit version number (e.g., 001, 002, 010).
    
    Args:
        migrations_dir: Path to migrations directory
        
    Returns:
        List of (version, file_path) tuples, sorted by version number
        
    Raises:
        ValueError: If migration files have invalid naming or duplicate versions
    """
    if not migrations_dir.exists():
        raise ValueError(f"Migrations directory does not exist: {migrations_dir}")
    
    migrations = []
    seen_versions = set()
    
    for file_path in migrations_dir.glob("*.py"):
        filename = file_path.name
        
        # Skip __init__.py and other non-migration files
        if filename.startswith("__"):
            continue
        
        # Extract version from filename (NNN_*.py)
        if not filename[0:3].isdigit():
            print(f"Warning: Skipping file '{filename}' - does not start with 3-digit version", file=sys.stderr)
            continue
        
        version = filename[0:3]
        
        if version in seen_versions:
            raise ValueError(f"Duplicate migration version '{version}' found in '{filename}'")
        
        seen_versions.add(version)
        migrations.append((version, file_path))
    
    # Sort by version number (lexicographic sort works for zero-padded numbers)
    migrations.sort(key=lambda x: x[0])
    
    return migrations


def load_migration_module(file_path: Path) -> tuple[str, Callable[[Database], None]]:
    """
    Load a migration module and extract VERSION and up() function.
    
    Args:
        file_path: Path to migration Python file
        
    Returns:
        Tuple of (version, up_function)
        
    Raises:
        ValueError: If migration file doesn't conform to expected interface
        ImportError: If migration file cannot be imported
    """
    # Load module dynamically
    spec = importlib.util.spec_from_file_location("migration", file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load migration module from {file_path}")
    
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    # Validate VERSION constant exists
    if not hasattr(module, "VERSION"):
        raise ValueError(f"Migration '{file_path.name}' missing VERSION constant")
    
    version = module.VERSION
    if not isinstance(version, str):
        raise ValueError(f"Migration '{file_path.name}' VERSION must be a string, got {type(version).__name__}")
    
    # Validate up() function exists
    if not hasattr(module, "up"):
        raise ValueError(f"Migration '{file_path.name}' missing up() function")
    
    up_func = module.up
    if not callable(up_func):
        raise ValueError(f"Migration '{file_path.name}' up must be callable, got {type(up_func).__name__}")
    
    return version, up_func


def ensure_schema_migrations_collection(db: Database) -> None:
    """
    Ensure schema_migrations collection exists with unique index on version.
    
    Args:
        db: MongoDB database instance
    """
    try:
        db.create_collection("schema_migrations")
    except CollectionInvalid:
        # Collection already exists, which is fine
        pass
    
    # Ensure unique index on version
    collection = db["schema_migrations"]
    try:
        collection.create_index("version", unique=True)
    except OperationFailure:
        # Index might already exist, which is fine
        pass


def get_applied_versions(db: Database) -> set[str]:
    """
    Get set of already-applied migration versions.
    
    Args:
        db: MongoDB database instance
        
    Returns:
        Set of version strings that have been applied
    """
    collection = db["schema_migrations"]
    applied = collection.find({}, {"version": 1})
    return {doc["version"] for doc in applied}


def apply_migration(db: Database, version: str, up_func: Callable[[Database], None]) -> None:
    """
    Apply a single migration and record it in schema_migrations.
    
    Args:
        db: MongoDB database instance
        version: Migration version string
        up_func: Migration up() function
        
    Raises:
        Exception: If migration fails (will be re-raised, migration not recorded)
    """
    start_time = time.time()
    
    try:
        # Execute migration
        up_func(db)
        
        # Record successful migration
        duration_ms = int((time.time() - start_time) * 1000)
        db["schema_migrations"].insert_one({
            "version": version,
            "applied_at": datetime.now(timezone.utc),
            "duration_ms": duration_ms,
        })
        
        print(f"Applied migration {version} (took {duration_ms}ms)")
        
    except Exception as e:
        # Migration failed - do not record in schema_migrations
        # This allows retry on next startup
        print(f"Migration {version} failed: {e}", file=sys.stderr)
        raise


def main() -> int:
    """
    Main migration runner entry point.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        # Load MongoDB settings
        settings = MongoSettings()
        
        # Connect to MongoDB
        client = MongoClient(
            settings.connection_string,
            serverSelectionTimeoutMS=10000,
        )
        
        try:
            db = client[settings.database]
            
            # Ensure schema_migrations collection exists
            ensure_schema_migrations_collection(db)
            
            # Discover migrations
            # Handle both local execution and container execution
            script_dir = Path(__file__).parent.absolute()
            if (script_dir.parent / "services" / "mongodb" / "migrations").exists():
                migrations_dir = script_dir.parent / "services" / "mongodb" / "migrations"
            else:
                # Container execution: /app/services/mongodb/migrations
                migrations_dir = Path("/app/services/mongodb/migrations")
            migrations = discover_migrations(migrations_dir)
            
            if not migrations:
                print("No migrations found", file=sys.stderr)
                return 1
            
            print(f"Discovered {len(migrations)} migration(s)")
            
            # Get already-applied versions
            applied_versions = get_applied_versions(db)
            
            # Apply migrations in order
            for version, file_path in migrations:
                if version in applied_versions:
                    print(f"Skipping migration {version}: already applied")
                    continue
                
                print(f"Applying migration {version} from {file_path.name}...")
                
                # Load migration module
                migration_version, up_func = load_migration_module(file_path)
                
                # Validate version matches filename
                if migration_version != version:
                    raise ValueError(
                        f"Migration '{file_path.name}' VERSION '{migration_version}' "
                        f"does not match filename version '{version}'"
                    )
                
                # Apply migration
                apply_migration(db, version, up_func)
            
            print("All migrations applied successfully")
            return 0
            
        finally:
            client.close()
            
    except Exception as e:
        print(f"Migration failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

