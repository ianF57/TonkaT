from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.backtest import router as backtest_router
from app.api.data import router as data_router
from app.api.health import router as health_router
from app.api.rank import router as rank_router
from app.api.regime import router as regime_router
from app.api.replay import router as replay_router
from app.api.signals import router as signals_router
from app.api.validate import router as validate_router
from app.ui.router import router as ui_router
from config import settings


def create_app() -> FastAPI:
    """Application factory for the Assemblief dashboard service."""
    app = FastAPI(title=settings.app_name)
    app.include_router(ui_router)
    app.include_router(health_router)
    app.include_router(data_router)
    app.include_router(regime_router)
    app.include_router(signals_router)
    app.include_router(backtest_router)
    app.include_router(rank_router)
    app.include_router(replay_router)
    app.include_router(validate_router)
    app.mount("/static", StaticFiles(directory="app/ui/static"), name="static")
    return app
