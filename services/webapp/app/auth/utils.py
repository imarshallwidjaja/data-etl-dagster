from typing import Optional

from fastapi import Request


def get_client_ip(request: Request) -> Optional[str]:
    """Extract client IP from X-Forwarded-For header or request.client.host."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return None
