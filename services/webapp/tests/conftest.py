# =============================================================================
# Webapp conftest.py - Shared test fixtures
# =============================================================================

import sys
from pathlib import Path

import pytest

# Add webapp app to path for imports
WEBAPP_DIR = Path(__file__).parent.parent / "app"
sys.path.insert(0, str(WEBAPP_DIR.parent))
