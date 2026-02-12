# =============================================================================
# Session Helper Unit Tests
# =============================================================================
# TDD tests for session helpers: new_sid, new_csrf_token, set_session_user.
# =============================================================================

import pytest

from app.auth.session import new_sid, new_csrf_token, set_session_user


class TestNewSid:
    """Tests for new_sid() session identifier generation."""

    def test_returns_non_empty_string(self):
        """new_sid() must produce a non-empty token."""
        sid = new_sid()
        assert isinstance(sid, str)
        assert len(sid) > 0

    def test_returns_unique_values(self):
        """Successive calls produce distinct identifiers."""
        sids = {new_sid() for _ in range(50)}
        assert len(sids) == 50

    def test_is_url_safe(self):
        """Token should only contain URL-safe characters."""
        sid = new_sid()
        # secrets.token_urlsafe produces [A-Za-z0-9_-]
        import re

        assert re.fullmatch(r"[A-Za-z0-9_-]+", sid)


class TestNewCsrfToken:
    """Tests for new_csrf_token() generation."""

    def test_returns_non_empty_string(self):
        """new_csrf_token() must produce a non-empty token."""
        token = new_csrf_token()
        assert isinstance(token, str)
        assert len(token) > 0

    def test_returns_unique_values(self):
        """Successive calls produce distinct tokens."""
        tokens = {new_csrf_token() for _ in range(50)}
        assert len(tokens) == 50

    def test_is_url_safe(self):
        """Token should only contain URL-safe characters."""
        token = new_csrf_token()
        import re

        assert re.fullmatch(r"[A-Za-z0-9_-]+", token)


class TestSetSessionUser:
    """Tests for set_session_user() session initialization."""

    def test_sets_user_key(self):
        """Session should contain the username under 'user' key."""
        session: dict = {}
        set_session_user(session, "alice")
        assert session["user"] == "alice"

    def test_sets_sid_key(self):
        """Session should contain a non-empty 'sid' key."""
        session: dict = {}
        set_session_user(session, "alice")
        assert "sid" in session
        assert isinstance(session["sid"], str)
        assert len(session["sid"]) > 0

    def test_sets_csrf_key(self):
        """Session should contain a non-empty 'csrf' key."""
        session: dict = {}
        set_session_user(session, "alice")
        assert "csrf" in session
        assert isinstance(session["csrf"], str)
        assert len(session["csrf"]) > 0

    def test_clears_old_session_keys(self):
        """Pre-existing session keys must be cleared before setting new user."""
        session: dict = {
            "user": "old_user",
            "sid": "old_sid",
            "csrf": "old_csrf",
            "junk": "leftover",
        }
        set_session_user(session, "bob")
        assert session["user"] == "bob"
        assert session["sid"] != "old_sid"
        assert session["csrf"] != "old_csrf"
        assert "junk" not in session

    def test_rotates_sid_on_each_call(self):
        """Each call to set_session_user should produce a fresh sid (session fixation protection)."""
        session: dict = {}
        set_session_user(session, "alice")
        first_sid = session["sid"]

        set_session_user(session, "alice")
        second_sid = session["sid"]

        assert first_sid != second_sid

    def test_rotates_csrf_on_each_call(self):
        """Each call to set_session_user should produce a fresh csrf token."""
        session: dict = {}
        set_session_user(session, "alice")
        first_csrf = session["csrf"]

        set_session_user(session, "alice")
        second_csrf = session["csrf"]

        assert first_csrf != second_csrf

    def test_session_contains_only_expected_keys(self):
        """After set_session_user, session should contain exactly user, sid, and csrf."""
        session: dict = {}
        set_session_user(session, "alice")
        assert set(session.keys()) == {"user", "sid", "csrf"}
