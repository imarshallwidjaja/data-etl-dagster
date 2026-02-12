# =============================================================================
# Session Helpers
# =============================================================================
# Utilities for managing signed session data: session IDs, CSRF tokens,
# and session-user initialization with fixation protection.
# =============================================================================

import secrets


def new_sid() -> str:
    """Generate a cryptographically-random, URL-safe session identifier."""
    return secrets.token_urlsafe(32)


def new_csrf_token() -> str:
    """Generate a cryptographically-random, URL-safe CSRF token."""
    return secrets.token_urlsafe(32)


def set_session_user(session: dict, username: str) -> None:
    """
    Initialize (or re-initialize) a session for the given user.

    Clears any pre-existing keys (session-fixation protection),
    then sets ``user``, ``sid``, and ``csrf`` with fresh values.
    """
    session.clear()
    session["user"] = username
    session["sid"] = new_sid()
    session["csrf"] = new_csrf_token()
