"""
Unit tests for MongoDB migration runner.

Tests migration discovery and loading logic without requiring MongoDB connection.
"""
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Add scripts directory to path for imports
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir.parent))

from scripts.migrate_db import discover_migrations, load_migration_module


class TestMigrationDiscovery:
    """Test migration file discovery."""
    
    def test_discover_migrations_finds_valid_files(self):
        """Test that valid migration files are discovered."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            
            # Create valid migration files
            (migrations_dir / "001_first_migration.py").write_text(
                'VERSION = "001"\n'
                'def up(db):\n'
                '    pass\n'
            )
            (migrations_dir / "002_second_migration.py").write_text(
                'VERSION = "002"\n'
                'def up(db):\n'
                '    pass\n'
            )
            (migrations_dir / "010_third_migration.py").write_text(
                'VERSION = "010"\n'
                'def up(db):\n'
                '    pass\n'
            )
            
            # Create non-migration files (should be ignored)
            (migrations_dir / "__init__.py").write_text("")
            (migrations_dir / "invalid.py").write_text("")
            
            migrations = discover_migrations(migrations_dir)
            
            # Should find 3 migrations, sorted by version
            assert len(migrations) == 3
            versions = [v for v, _ in migrations]
            assert versions == ["001", "002", "010"]
    
    def test_discover_migrations_raises_on_duplicate_versions(self):
        """Test that duplicate versions raise ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            
            (migrations_dir / "001_first.py").write_text('VERSION = "001"\ndef up(db): pass\n')
            (migrations_dir / "001_duplicate.py").write_text('VERSION = "001"\ndef up(db): pass\n')
            
            with pytest.raises(ValueError, match="Duplicate migration version"):
                discover_migrations(migrations_dir)
    
    def test_discover_migrations_raises_on_missing_directory(self):
        """Test that missing directory raises ValueError."""
        non_existent = Path("/non/existent/path")
        
        with pytest.raises(ValueError, match="does not exist"):
            discover_migrations(non_existent)


class TestMigrationLoading:
    """Test migration module loading."""
    
    def test_load_migration_module_valid(self):
        """Test loading a valid migration module."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migration_file = Path(tmpdir) / "001_test.py"
            migration_file.write_text(
                'VERSION = "001"\n'
                'def up(db):\n'
                '    """Migration function."""\n'
                '    pass\n'
            )
            
            version, up_func = load_migration_module(migration_file)
            
            assert version == "001"
            assert callable(up_func)
    
    def test_load_migration_module_missing_version(self):
        """Test that missing VERSION raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migration_file = Path(tmpdir) / "001_test.py"
            migration_file.write_text('def up(db): pass\n')
            
            with pytest.raises(ValueError, match="missing VERSION"):
                load_migration_module(migration_file)
    
    def test_load_migration_module_missing_up(self):
        """Test that missing up() function raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migration_file = Path(tmpdir) / "001_test.py"
            migration_file.write_text('VERSION = "001"\n')
            
            with pytest.raises(ValueError, match="missing up"):
                load_migration_module(migration_file)
    
    def test_load_migration_module_invalid_version_type(self):
        """Test that non-string VERSION raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migration_file = Path(tmpdir) / "001_test.py"
            migration_file.write_text('VERSION = 1\ndef up(db): pass\n')
            
            with pytest.raises(ValueError, match="VERSION must be a string"):
                load_migration_module(migration_file)

