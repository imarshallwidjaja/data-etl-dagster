# =============================================================================
# CSRF Enforcement Dependency
# =============================================================================
# FastAPI dependency that validates a synchronizer CSRF token on unsafe
# requests (POST, PUT, PATCH, DELETE).  Enforcement is only active when
# a session cookie is present (i.e. the user is authenticated via a
# browser session).
#
# The token is accepted from:
#   1. ``X-CSRF-Token`` header  (fetch / HTMX)
#   2. ``csrf_token`` form field (HTML forms)
#
# Comparison uses ``hmac.compare_digest`` for timing-safe equality.
# =============================================================================

import hmac
import logging

from fastapi import HTTPException, Request, status

logger = logging.getLogger(__name__)


async def require_csrf(request: Request) -> None:
    """
    Validate the CSRF token for state-changing requests.

    Skips enforcement when no session cookie is present (the request will
    be rejected by ``get_current_user`` instead).

    Raises ``403 Forbidden`` when the token is missing or invalid.
    """
    # Only enforce on unsafe methods.
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return

    # Only enforce when the user has an active session (i.e. browser auth).
    session_csrf: str = request.session.get("csrf", "")
    if not session_csrf:
        # No session CSRF → the user is not authenticated via session.
        # Let get_current_user handle 401/redirect.
        return

    # --- Collect submitted token ---
    submitted: str = ""

    # 1. Header (fetch / HTMX)
    submitted = request.headers.get("X-CSRF-Token", "")

    # 2. Form field fallback — only when header is absent and content-type
    #    indicates a form submission.
    if not submitted:
        content_type = request.headers.get("content-type", "")
        if (
            "application/x-www-form-urlencoded" in content_type
            or "multipart/form-data" in content_type
        ):
            form = await request.form()
            submitted = form.get("csrf_token", "")  # type: ignore[assignment]

    if not submitted or not hmac.compare_digest(submitted, session_csrf):
        logger.warning(
            "CSRF validation failed for %s %s",
            request.method,
            request.url.path,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF validation failed",
        )
