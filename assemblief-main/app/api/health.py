from __future__ import annotations

from datetime import datetime, UTC

from fastapi import APIRouter, Depends
from app.api.auth import require_api_key

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/health")
def health() -> dict[str, str]:
    """Return process-level service health."""
    return {
        "status": "ok",
        "service": "assemblief-api",
        "timestamp": datetime.now(UTC).isoformat(),
    }
