from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from signal_noise.config import CACHE_DIR, RAW_DIR, DEFAULT_COLLECTOR, CollectorConfig

if TYPE_CHECKING:
    from signal_noise.store.sqlite_store import SignalStore

log = logging.getLogger(__name__)


# ── Taxonomy ──
# domain: stable top-level grouping (rarely changes)
DOMAINS = {
    "markets",       # equities, FX, bonds, commodities, crypto, derivatives
    "economy",       # GDP, employment, inflation, trade, housing, agriculture, energy
    "environment",   # weather, climate, ocean, geology, space weather, biodiversity
    "technology",    # software ecosystems, internet, logistics, aviation, R&D
    "sentiment",     # fear/greed, social media, attention, prediction markets
    "society",       # health, mortality, conflict, demographics
}

# category: concrete data classification
CATEGORIES = {
    # markets
    "equity", "crypto", "crypto_derivatives", "forex", "rates", "commodity",
    "microstructure", "regulatory",
    # economy
    "economic", "labor", "inflation", "trade", "fiscal",
    "real_estate", "food_price", "agriculture", "energy",
    # environment
    "weather", "climate", "marine", "air_quality", "hydrology", "satellite",
    "space_weather", "seismic", "celestial",
    "wildlife", "biodiversity",
    # technology
    "developer", "academic", "patents",
    "logistics", "aviation", "internet", "space",
    "transportation", "safety",
    # sentiment
    "sentiment", "attention", "prediction_market", "temporal",
    # society
    "epidemiology", "public_health", "excess_deaths", "cause_of_death",
    "armed_conflict", "displacement", "city_stats",
}

# update_frequency: allowed values
FREQUENCIES = {"hourly", "daily", "weekly", "monthly", "quarterly", "yearly", "annual"}

FREQUENCY_TO_SECONDS: dict[str, int] = {
    "hourly": 3600,
    "daily": 86400,
    "weekly": 604800,
    "monthly": 2592000,
    "quarterly": 7776000,
    "yearly": 31536000,
    "annual": 31536000,
}

# Default collect intervals by collection level (seconds)
LEVEL_DEFAULT_INTERVALS: dict[str, int] = {
    "L5": 300,  # active probes: every 5 min
}


@dataclass
class CollectorMeta:
    name: str
    display_name: str
    update_frequency: str
    api_docs_url: str
    requires_key: bool = False
    domain: str = ""       # top-level grouping
    category: str = ""     # concrete classification
    signal_type: str = "scalar"  # "scalar" or "ohlcv"
    collection_level: str = ""   # L1-L7 (empty = auto-detect)
    collect_interval: int = 0    # seconds (0 = auto from level/frequency)

    @property
    def interval(self) -> int:
        if self.collect_interval > 0:
            return self.collect_interval
        if self.collection_level and self.collection_level in LEVEL_DEFAULT_INTERVALS:
            return LEVEL_DEFAULT_INTERVALS[self.collection_level]
        return FREQUENCY_TO_SECONDS[self.update_frequency]


class BaseCollector(ABC):
    meta: CollectorMeta

    def __init__(self, config: CollectorConfig | None = None):
        self.config = config or DEFAULT_COLLECTOR

    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        """Fetch data from provider. Return DataFrame with [timestamp|date, value]."""
        ...

    def cache_path(self) -> Path:
        return CACHE_DIR / f"{self.meta.name}.json"

    def parquet_path(self) -> Path:
        return RAW_DIR / f"{self.meta.name}.parquet"

    def collect(self, store: SignalStore | None = None) -> pd.DataFrame:
        cache = self.cache_path()
        if cache.exists():
            age_hours = (time.time() - cache.stat().st_mtime) / 3600
            if age_hours < self.config.cache_max_age_hours:
                log.debug("Using cache for %s (%.1fh old)", self.meta.name, age_hours)
                if store:
                    df = store.get_data(self.meta.name)
                    if not df.empty:
                        return df
                return self._read_parquet()

        df = self._fetch_with_retry()
        self._save_cache(df)
        if store:
            store.save(self.meta.name, df)
            store.save_meta(
                self.meta.name, self.meta.domain,
                self.meta.category, self.meta.interval,
                self.meta.signal_type,
            )
        else:
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
