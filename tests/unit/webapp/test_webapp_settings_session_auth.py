# =============================================================================
# Session Auth Settings Unit Tests
# =============================================================================
# Tests for session/auth settings validation in webapp config.
# =============================================================================

import pytest
from pydantic import ValidationError

from app.config import Settings


class TestSessionSecretValidation:
    """WEBAPP_SESSION_SECRET is required outside development."""

    def test_ci_environment_missing_secret_raises(self):
        """ENVIRONMENT=ci and WEBAPP_SESSION_SECRET missing → raises."""
        with pytest.raises(ValidationError, match="(?i)session.?secret"):
            Settings(
                environment="ci",
                webapp_session_secret=None,
                _env_file=None,
            )

    def test_staging_environment_missing_secret_raises(self):
        """ENVIRONMENT=staging and WEBAPP_SESSION_SECRET missing → raises."""
        with pytest.raises(ValidationError, match="(?i)session.?secret"):
            Settings(
                environment="staging",
                webapp_session_secret=None,
                _env_file=None,
            )

    def test_production_environment_missing_secret_raises(self):
        """ENVIRONMENT=production and WEBAPP_SESSION_SECRET missing → raises."""
        with pytest.raises(ValidationError, match="(?i)session.?secret"):
            Settings(
                environment="production",
                webapp_session_secret=None,
                _env_file=None,
            )

    def test_development_environment_missing_secret_succeeds(self):
        """ENVIRONMENT=development and secret missing → succeeds with dev default."""
        settings = Settings(
            environment="development",
            webapp_session_secret=None,
            _env_file=None,
        )
        assert settings.webapp_session_secret is not None
        assert len(settings.webapp_session_secret) > 0

    def test_development_default_secret_is_stable(self):
        """Dev default secret is consistent across instantiations."""
        s1 = Settings(
            environment="development", webapp_session_secret=None, _env_file=None
        )
        s2 = Settings(
            environment="development", webapp_session_secret=None, _env_file=None
        )
        assert s1.webapp_session_secret == s2.webapp_session_secret

    def test_explicit_secret_used_when_provided(self):
        """Explicit secret is used regardless of environment."""
        settings = Settings(
            environment="production",
            webapp_session_secret="my-prod-secret",
            _env_file=None,
        )
        assert settings.webapp_session_secret == "my-prod-secret"


class TestAuthModeLiteral:
    """WEBAPP_AUTH_MODE accepts only session|hybrid."""

    def test_session_mode_accepted(self):
        """session mode is valid."""
        settings = Settings(
            environment="development",
            webapp_auth_mode="session",
            _env_file=None,
        )
        assert settings.webapp_auth_mode == "session"

    def test_hybrid_mode_accepted(self):
        """hybrid mode is valid."""
        settings = Settings(
            environment="development",
            webapp_auth_mode="hybrid",
            _env_file=None,
        )
        assert settings.webapp_auth_mode == "hybrid"

    def test_invalid_mode_rejected(self):
        """Invalid mode raises ValidationError."""
        with pytest.raises(ValidationError, match="(?i)auth.?mode"):
            Settings(
                environment="development",
                webapp_auth_mode="basic",
                _env_file=None,
            )


class TestSessionFieldDefaults:
    """Default values for session fields."""

    def test_default_auth_mode_is_session(self):
        """Default auth mode is 'session'."""
        settings = Settings(environment="development", _env_file=None)
        assert settings.webapp_auth_mode == "session"

    def test_default_max_age_seconds(self):
        """Default session max age is 28800 (8 hours)."""
        settings = Settings(environment="development", _env_file=None)
        assert settings.webapp_session_max_age_seconds == 28800

    def test_default_session_secure_false(self):
        """Default session secure is False."""
        settings = Settings(environment="development", _env_file=None)
        assert settings.webapp_session_secure is False

    def test_default_cookie_name(self):
        """Default cookie name is 'webapp_session'."""
        settings = Settings(environment="development", _env_file=None)
        assert settings.webapp_session_cookie_name == "webapp_session"

    def test_default_environment_is_development(self):
        """Default environment is 'development'."""
        settings = Settings(_env_file=None)
        assert settings.environment == "development"
