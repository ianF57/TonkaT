from __future__ import annotations

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import settings

bearer_scheme = HTTPBearer(auto_error=False)


def require_api_key(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> None:
    """Validate bearer token against configured API key.

    If API_KEY is not configured, auth is treated as disabled for local/dev use.
    """
    expected_token = settings.api_key.strip()
    if not expected_token:
        return

    token = credentials.credentials if credentials else None
    if not token or token != expected_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API token")
