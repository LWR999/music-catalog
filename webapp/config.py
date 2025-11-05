import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    # Paths
    DB_PATH: str = os.environ.get("MC_DB_PATH", "/var/lib/music-catalog/catalog.db")
    CONFIG_PATH: str = os.environ.get("MC_CONFIG_PATH", "/home/dl/development/.config/music-catalog/dev.yaml")

    # Job behavior
    DEEP_LIMIT: int | None = int(os.environ.get("MC_DEEP_LIMIT", "0")) or None
    DEBounce_SEC: int = int(os.environ.get("MC_DEBOUNCE_SEC", "10"))

settings = Settings()
