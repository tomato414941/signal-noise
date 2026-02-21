from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from signal_noise.config import CACHE_DIR, RAW_DIR, DEFAULT_COLLECTOR, CollectorConfig

log = logging.getLogger(__name__)


# ── Taxonomy ──
# domain: stable top-level grouping (rarely changes)
DOMAINS = {
    "financial",     # equities, FX, bonds, commodities, crypto
    "macro",         # GDP, employment, inflation, trade, fiscal
    "sentiment",     # fear/greed, social media, attention
    "earth",         # weather, climate, ocean, air quality, hydrology
    "geophysical",   # solar, geomagnetic, seismic, lunar, volcanic
    "infrastructure",# logistics, aviation, shipping, internet
    "real_estate",   # housing, property prices, REITs
    "developer",     # GitHub, npm, StackOverflow, tech trends
}

# category: concrete data classification
CATEGORIES = {
    # financial
    "equity", "crypto", "forex", "rates", "commodity",
    # macro
    "economic", "labor", "inflation", "trade", "fiscal",
    # sentiment
    "sentiment", "attention",
    # earth
    "weather", "climate", "marine", "air_quality", "hydrology", "satellite",
    # geophysical
    "space_weather", "seismic", "celestial",
    # infrastructure
    "logistics", "aviation",
    # real_estate
    "real_estate",
    # developer
    "developer",
    # misc
    "temporal",
}

# update_frequency: allowed values
FREQUENCIES = {"hourly", "daily", "weekly", "monthly", "quarterly", "yearly"}


@dataclass
class SourceMeta:
    name: str
    display_name: str
    update_frequency: str
    data_type: str         # category (kept for backward compat)
    api_docs_url: str
    requires_key: bool = False
    domain: str = ""       # top-level grouping
    category: str = ""     # concrete classification (== data_type when set)


class BaseCollector(ABC):
    meta: SourceMeta

    def __init__(self, config: CollectorConfig | None = None):
        self.config = config or DEFAULT_COLLECTOR

    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        """Fetch data from source. Return DataFrame with [timestamp|date, value]."""
        ...

    def cache_path(self) -> Path:
        return CACHE_DIR / f"{self.meta.name}.json"

    def parquet_path(self) -> Path:
        return RAW_DIR / f"{self.meta.name}.parquet"

    def collect(self) -> pd.DataFrame:
        cache = self.cache_path()
        if cache.exists():
            age_hours = (time.time() - cache.stat().st_mtime) / 3600
            if age_hours < self.config.cache_max_age_hours:
                log.debug("Using cache for %s (%.1fh old)", self.meta.name, age_hours)
                return self._read_parquet()

        df = self._fetch_with_retry()
        self._save_cache(df)
        self._save_parquet(df)
        return df

    def _fetch_with_retry(self) -> pd.DataFrame:
        last_err: Exception | None = None
        for attempt in range(self.config.max_retries):
            try:
                return self.fetch()
            except Exception as e:
                last_err = e
                wait = self.config.retry_backoff_base ** attempt
                log.warning(
                    "%s fetch failed (attempt %d/%d): %s. Retry in %.1fs",
                    self.meta.name, attempt + 1, self.config.max_retries, e, wait,
                )
                if attempt < self.config.max_retries - 1:
                    time.sleep(wait)
        raise RuntimeError(
            f"Failed to fetch {self.meta.name} after {self.config.max_retries} attempts"
        ) from last_err

    def _save_cache(self, df: pd.DataFrame) -> None:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        records = df.copy()
        for col in records.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
            records[col] = records[col].astype(str)
        self.cache_path().write_text(json.dumps(records.to_dict(orient="records")))

    def _save_parquet(self, df: pd.DataFrame) -> None:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        path = self.parquet_path()
        if path.exists():
            existing = pd.read_parquet(path)
            ts_col = "timestamp" if "timestamp" in df.columns else "date"
            combined = pd.concat([existing, df]).drop_duplicates(subset=[ts_col], keep="last")
            combined = combined.sort_values(ts_col).reset_index(drop=True)
            combined.to_parquet(path, index=False)
        else:
            df.to_parquet(path, index=False)
        log.info("Saved %s: %d rows -> %s", self.meta.name, len(df), path)

    def _read_parquet(self) -> pd.DataFrame:
        path = self.parquet_path()
        if path.exists():
            return pd.read_parquet(path)
        return pd.DataFrame()

    def status(self) -> dict:
        parquet = self.parquet_path()
        cache = self.cache_path()
        rows = 0
        if parquet.exists():
            rows = len(pd.read_parquet(parquet))
        return {
            "name": self.meta.name,
            "display_name": self.meta.display_name,
            "has_data": parquet.exists(),
            "rows": rows,
            "cache_age_hours": (
                (time.time() - cache.stat().st_mtime) / 3600 if cache.exists() else None
            ),
            "requires_key": self.meta.requires_key,
        }
