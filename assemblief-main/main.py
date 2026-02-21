from __future__ import annotations

import logging
import threading
import webbrowser

import uvicorn

from app.app_factory import create_app
from app.data.database import initialize_database
from app.logging_setup import configure_logging
from config import settings

logger = logging.getLogger(__name__)
app = create_app()


def _open_browser() -> None:
    """Open browser after startup in local runtime."""
    url = f"http://{settings.app_host}:{settings.app_port}/"
    opened = webbrowser.open(url)
    if opened:
        logger.info("Opened browser at %s", url)
    else:
        logger.warning("Could not auto-open browser. Visit %s manually.", url)


if __name__ == "__main__":
    configure_logging(settings.log_level)
    logger.info("Initializing database at %s", settings.db_path)
    initialize_database()
    threading.Timer(1.0, _open_browser).start()
    logger.info("Starting %s on %s:%s", settings.app_name, settings.app_host, settings.app_port)
    uvicorn.run(app, host=settings.app_host, port=settings.app_port, log_level=settings.log_level.lower())
