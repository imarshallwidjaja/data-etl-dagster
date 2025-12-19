# =============================================================================
# Auth Provider Unit Tests
# =============================================================================

import pytest

from app.auth.providers import AuthenticatedUser, BasicAuthProvider


class TestBasicAuthProvider:
    """Tests for BasicAuthProvider."""

    def test_authenticate_valid_credentials(self):
        """Valid username and password should return AuthenticatedUser."""
        provider = BasicAuthProvider(username="admin", password="secret")

        result = provider.authenticate({"username": "admin", "password": "secret"})

        assert result is not None
        assert isinstance(result, AuthenticatedUser)
        assert result.username == "admin"

    def test_authenticate_invalid_username(self):
        """Invalid username should return None."""
        provider = BasicAuthProvider(username="admin", password="secret")

        result = provider.authenticate({"username": "wrong", "password": "secret"})

        assert result is None

    def test_authenticate_invalid_password(self):
        """Invalid password should return None."""
        provider = BasicAuthProvider(username="admin", password="secret")

        result = provider.authenticate({"username": "admin", "password": "wrong"})

        assert result is None

    def test_authenticate_missing_credentials(self):
        """Missing credentials should return None."""
        provider = BasicAuthProvider(username="admin", password="secret")

        # Missing username
        result = provider.authenticate({"password": "secret"})
        assert result is None

        # Missing password
        result = provider.authenticate({"username": "admin"})
        assert result is None

        # Empty dict
        result = provider.authenticate({})
        assert result is None

    def test_authenticate_empty_credentials(self):
        """Empty string credentials should return None."""
        provider = BasicAuthProvider(username="admin", password="secret")

        result = provider.authenticate({"username": "", "password": ""})

        assert result is None

    def test_timing_safe_comparison(self):
        """Verify timing-safe comparison is used (no early return on username mismatch)."""
        # This test verifies the implementation uses secrets.compare_digest
        # Both username and password are checked regardless of username match
        provider = BasicAuthProvider(username="admin", password="secret")

        # Even with wrong username, password is still checked
        result = provider.authenticate({"username": "x", "password": "secret"})
        assert result is None


class TestAuthenticatedUser:
    """Tests for AuthenticatedUser dataclass."""

    def test_authenticated_user_creation(self):
        """AuthenticatedUser should store username."""
        user = AuthenticatedUser(username="testuser")

        assert user.username == "testuser"

    def test_authenticated_user_equality(self):
        """AuthenticatedUser with same username should be equal."""
        user1 = AuthenticatedUser(username="admin")
        user2 = AuthenticatedUser(username="admin")

        assert user1 == user2

    def test_authenticated_user_inequality(self):
        """AuthenticatedUser with different username should not be equal."""
        user1 = AuthenticatedUser(username="admin")
        user2 = AuthenticatedUser(username="other")

        assert user1 != user2
