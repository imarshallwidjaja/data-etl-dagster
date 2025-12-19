# =============================================================================
# Authentication Providers
# =============================================================================
# Abstract provider interface with Basic Auth implementation.
# Designed for future SSO extensibility.
# =============================================================================

import secrets
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class AuthenticatedUser:
    """Represents an authenticated user."""

    username: str
    display_name: Optional[str] = None

    def __post_init__(self) -> None:
        if self.display_name is None:
            self.display_name = self.username


class AuthProvider(ABC):
    """Abstract authentication provider interface."""

    @abstractmethod
    def authenticate(self, credentials: dict) -> Optional[AuthenticatedUser]:
        """
        Authenticate a user with the given credentials.

        Args:
            credentials: Dictionary containing authentication details.
                         Structure varies by provider type.

        Returns:
            AuthenticatedUser if authentication succeeds, None otherwise.
        """
        ...


class BasicAuthProvider(AuthProvider):
    """HTTP Basic Authentication provider."""

    def __init__(self, username: str, password: str) -> None:
        """
        Initialize with expected credentials.

        Args:
            username: Expected username
            password: Expected password
        """
        self._username = username
        self._password = password

    def authenticate(self, credentials: dict) -> Optional[AuthenticatedUser]:
        """
        Authenticate using HTTP Basic Auth credentials.

        Args:
            credentials: Dict with 'username' and 'password' keys.

        Returns:
            AuthenticatedUser if credentials match, None otherwise.
        """
        username = credentials.get("username", "")
        password = credentials.get("password", "")

        # Use constant-time comparison to prevent timing attacks
        username_match = secrets.compare_digest(username, self._username)
        password_match = secrets.compare_digest(password, self._password)

        if username_match and password_match:
            return AuthenticatedUser(username=username)

        return None


# Future: Add SSO providers here
# class OIDCAuthProvider(AuthProvider):
#     """OpenID Connect authentication provider."""
#     ...
