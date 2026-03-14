"""Have I Been Pwned (HIBP) data breach stats.

Tracks the cumulative number of known data breaches and total
compromised accounts. Trends reflect cybersecurity landscape shifts.
"""
from __future__ import annotations

import time

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://haveibeenpwned.com/api/v3/breaches"

_cache: list | None = None
_cache_ts: float = 0.0


def _fetch_breaches(timeout: int = 30) -> list[dict]:
    global _cache, _cache_ts
    now = time.monotonic()
    if _cache is not None and (now - _cache_ts) < 600:
        return _cache
    resp = requests.get(
        _API_URL,
        headers={"User-Agent": "signal-noise"},
        timeout=timeout,
    )
    resp.raise_for_status()
    _cache = resp.json()
    _cache_ts = now
    return _cache


class HIBPBreachCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="hibp_breach_count",
        display_name="HIBP Total Known Data Breaches",
        update_frequency="daily",
        api_docs_url="https://haveibeenpwned.com/API/v3",
        domain="technology",
        category="internet",
    )

    def fetch(self) -> pd.DataFrame:
        breaches = _fetch_breaches(timeout=self.config.request_timeout)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(len(breaches))}])


class HIBPPwnedAccountsCollector(BaseCollector):
    meta = CollectorMeta(
        name="hibp_pwned_accounts",
        display_name="HIBP Total Compromised Accounts",
        update_frequency="daily",
        api_docs_url="https://haveibeenpwned.com/API/v3",
        domain="technology",
        category="internet",
    )

    def fetch(self) -> pd.DataFrame:
        breaches = _fetch_breaches(timeout=self.config.request_timeout)
        total = sum(b.get("PwnCount", 0) for b in breaches)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])
