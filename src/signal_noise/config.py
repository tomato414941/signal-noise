from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"


@dataclass
class CollectorConfig:
    cache_max_age_hours: float = 12.0
    request_timeout: int = 30
    max_retries: int = 3
    retry_backoff_base: float = 2.0


DEFAULT_COLLECTOR = CollectorConfig()
