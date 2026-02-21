from __future__ import annotations

import logging
import socket
import threading
import webbrowser

import uvicorn

from app.app_factory import create_app
from app.data.database import initialize_database
from app.logging_setup import configure_logging
from config import settings

logger = logging.getLogger(__name__)
app = create_app()


def _open_browser(host: str, port: int) -> None:
    """Open browser after startup in local runtime."""
    url = f"http://{host}:{port}/"
    opened = webbrowser.open(url)
    if opened:
        logger.info("Opened browser at %s", url)
    else:
        logger.warning("Could not auto-open browser. Visit %s manually.", url)


def _can_bind(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
        except OSError:
            return False
    return True


def _select_available_port(host: str, preferred_port: int, max_tries: int = 20) -> int:
    """Pick the preferred port or the next available one."""
    if _can_bind(host, preferred_port):
        return preferred_port

    for offset in range(1, max_tries + 1):
        candidate = preferred_port + offset
        if _can_bind(host, candidate):
            logger.warning(
                "Port %s is already in use; falling back to port %s.",
                preferred_port,
                candidate,
            )
            return candidate

    raise RuntimeError(
        f"No available port found in range {preferred_port}-{preferred_port + max_tries}."
    )


if __name__ == "__main__":
    configure_logging(settings.log_level)
    logger.info("Initializing database at %s", settings.db_path)
    initialize_database()

    host = settings.app_host
    port = _select_available_port(host=host, preferred_port=settings.app_port)

    threading.Timer(1.0, _open_browser, args=(host, port)).start()
    logger.info("Starting %s on %s:%s", settings.app_name, host, port)
    uvicorn.run(app, host=host, port=port, log_level=settings.log_level.lower())
