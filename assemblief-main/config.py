from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    """Application runtime settings loaded from environment variables."""

    app_name: str = os.getenv("APP_NAME", "Assemblief Dashboard")
    app_host: str = os.getenv("APP_HOST", "127.0.0.1")
    app_port: int = int(os.getenv("APP_PORT", "8000"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    db_path: Path = Path(os.getenv("DB_PATH", "app/data/assemblief.db"))

    @property
    def database_url(self) -> str:
        return f"sqlite:///{self.db_path.as_posix()}"


settings = Settings()
