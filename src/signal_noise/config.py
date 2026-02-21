from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"
REPORTS_DIR = DATA_DIR / "reports"


@dataclass
class CollectorConfig:
    cache_max_age_hours: float = 12.0
    request_timeout: int = 30
    max_retries: int = 3
    retry_backoff_base: float = 2.0


@dataclass
class EvaluationConfig:
    return_periods: list[str] = field(default_factory=lambda: ["1h", "4h", "1d", "1w"])
    max_lag_periods: int = 24
    significance_level: float = 0.05
    correction_method: str = "fdr"


DEFAULT_COLLECTOR = CollectorConfig()
DEFAULT_EVALUATION = EvaluationConfig()
