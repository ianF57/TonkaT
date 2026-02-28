from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from fastapi import Header, HTTPException, status


@dataclass(frozen=True)
class UserContext:
    user_id: str
    tier: str


TIER_LIMITS = {
    "free": {"max_days": 365, "optimization": False, "walk_forward": False, "max_combinations": 0},
    "pro": {"max_days": 3650, "optimization": True, "walk_forward": True, "max_combinations": 500},
}


def get_user_context(
    x_user_id: str | None = Header(default="anonymous"),
    x_user_tier: str | None = Header(default="free"),
) -> UserContext:
    tier = (x_user_tier or "free").lower()
    if tier not in TIER_LIMITS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown user tier")
    return UserContext(user_id=x_user_id or "anonymous", tier=tier)


def enforce_tier(
    user: UserContext,
    start_date: datetime,
    end_date: datetime,
    optimization: bool,
    walk_forward: bool,
) -> dict[str, object]:
    limits = TIER_LIMITS[user.tier]
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="Invalid date range")

    if (end_date - start_date).days > int(limits["max_days"]):
        raise HTTPException(status_code=403, detail="Date range exceeds tier allowance")
    if optimization and not bool(limits["optimization"]):
        raise HTTPException(status_code=403, detail="Optimization requires Pro")
    if walk_forward and not bool(limits["walk_forward"]):
        raise HTTPException(status_code=403, detail="Walk-forward requires Pro")

    return limits
