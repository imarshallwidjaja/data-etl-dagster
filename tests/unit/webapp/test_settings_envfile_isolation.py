# =============================================================================
# Settings env_file isolation tests
# =============================================================================
# Verify that the webapp Settings class does NOT read a repo-root .env by
# default, so unit tests are not poisoned by local env files.
# =============================================================================

import os
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from app.config import Settings, get_settings


class TestSettingsEnvFileIsolation:
    """Settings must not load a CWD .env that could break tests."""

    def test_settings_does_not_read_cwd_env_file(self, tmp_path: Path):
        """Settings() ignores a .env file in the working directory.

        Reproduces: repo-root .env has ENVIRONMENT=ci without
        WEBAPP_SESSION_SECRET → importing main.py blows up.
        """
        # Create a poisoned .env in tmp_path
        env_file = tmp_path / ".env"
        env_file.write_text(
            textwrap.dedent("""\
                ENVIRONMENT=ci
                # No WEBAPP_SESSION_SECRET — would trigger validator
            """)
        )

        # Run from the directory containing the poisoned .env
        with patch.dict(os.environ, {}, clear=True):
            original_cwd = os.getcwd()
            try:
                os.chdir(tmp_path)
                # This must NOT raise, because Settings should not auto-read .env
                settings = Settings()
                # Without env file influence, environment defaults to "development"
                assert settings.environment == "development"
            finally:
                os.chdir(original_cwd)

    def test_get_settings_ignores_cwd_env_file(self, tmp_path: Path):
        """get_settings() also ignores CWD .env (cached singleton)."""
        env_file = tmp_path / ".env"
        env_file.write_text("ENVIRONMENT=staging\n")

        get_settings.cache_clear()
        try:
            with patch.dict(os.environ, {}, clear=True):
                original_cwd = os.getcwd()
                try:
                    os.chdir(tmp_path)
                    settings = get_settings()
                    assert settings.environment == "development"
                finally:
                    os.chdir(original_cwd)
        finally:
            get_settings.cache_clear()

    def test_env_vars_still_override_defaults(self):
        """Environment variables are still respected (just not .env files)."""
        with patch.dict(
            os.environ,
            {
                "ENVIRONMENT": "ci",
                "WEBAPP_SESSION_SECRET": "test-secret",
            },
            clear=False,
        ):
            settings = Settings(_env_file=None)
            assert settings.environment == "ci"
            assert settings.webapp_session_secret == "test-secret"
