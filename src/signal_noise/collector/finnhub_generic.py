from __future__ import annotations

import os
import time

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_FINNHUB_API_KEY: str | None = None
_BASE_URL = "https://finnhub.io/api/v1"
_finnhub_cache = SharedAPICache(ttl=3600)

# 1 year of daily data
_LOOKBACK_DAYS = 365


def _get_finnhub_key() -> str:
    global _FINNHUB_API_KEY
    if _FINNHUB_API_KEY:
        return _FINNHUB_API_KEY

    key = os.environ.get("FINNHUB_API_KEY")
    if not key:
        secret_path = os.path.expanduser("~/.secrets/finnhub")
        if os.path.exists(secret_path):
            with open(secret_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("export FINNHUB_API_KEY="):
                        key = line.split("=", 1)[1].strip().strip("'\"")
                        break
    if not key:
        raise RuntimeError(
            "FINNHUB_API_KEY not set. Get a free key at https://finnhub.io/register"
        )
    _FINNHUB_API_KEY = key
    return key


def _fetch_candle(symbol: str, resolution: str, timeout: int = 60) -> dict:
    cache_key = f"{symbol}|{resolution}"

    def _fetch() -> dict:
        api_key = _get_finnhub_key()
        now = int(time.time())
        start = now - _LOOKBACK_DAYS * 86400
        params = {
            "symbol": symbol,
            "resolution": resolution,
            "from": start,
            "to": now,
            "token": api_key,
        }
        resp = requests.get(
            f"{_BASE_URL}/stock/candle", params=params, timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()

    return _finnhub_cache.get_or_fetch(cache_key, _fetch)


# (symbol, collector_name, display_name, frequency, domain, category)
FINNHUB_SERIES: list[tuple[str, str, str, str, str, str]] = [
    # ── Magnificent 7 + major tech ──
    ("AAPL", "finnhub_aapl", "Finnhub: Apple (AAPL)", "daily", "financial", "equity"),
    ("MSFT", "finnhub_msft", "Finnhub: Microsoft (MSFT)", "daily", "financial", "equity"),
    ("GOOGL", "finnhub_googl", "Finnhub: Alphabet (GOOGL)", "daily", "financial", "equity"),
    ("AMZN", "finnhub_amzn", "Finnhub: Amazon (AMZN)", "daily", "financial", "equity"),
    ("NVDA", "finnhub_nvda", "Finnhub: NVIDIA (NVDA)", "daily", "financial", "equity"),
    ("TSLA", "finnhub_tsla", "Finnhub: Tesla (TSLA)", "daily", "financial", "equity"),
    ("META", "finnhub_meta", "Finnhub: Meta (META)", "daily", "financial", "equity"),
    # ── Sector ETFs (SPDR) ──
    ("XLF", "finnhub_xlf", "Finnhub: Financial Select SPDR (XLF)", "daily", "financial", "equity"),
    ("XLE", "finnhub_xle", "Finnhub: Energy Select SPDR (XLE)", "daily", "financial", "equity"),
    ("XLK", "finnhub_xlk", "Finnhub: Technology Select SPDR (XLK)", "daily", "financial", "equity"),
    ("XLV", "finnhub_xlv", "Finnhub: Health Care Select SPDR (XLV)", "daily", "financial", "equity"),
    ("XLI", "finnhub_xli", "Finnhub: Industrial Select SPDR (XLI)", "daily", "financial", "equity"),
    # ── Commodity ETFs ──
    ("GLD", "finnhub_gld", "Finnhub: SPDR Gold Shares (GLD)", "daily", "financial", "commodity"),
    ("SLV", "finnhub_slv", "Finnhub: iShares Silver (SLV)", "daily", "financial", "commodity"),
    ("USO", "finnhub_uso", "Finnhub: US Oil Fund (USO)", "daily", "financial", "commodity"),
    # ── Bond ETFs ──
    ("TLT", "finnhub_tlt", "Finnhub: 20+ Year Treasury (TLT)", "daily", "financial", "rates"),
    ("IEF", "finnhub_ief", "Finnhub: 7-10 Year Treasury (IEF)", "daily", "financial", "rates"),
    ("HYG", "finnhub_hyg", "Finnhub: High Yield Corporate (HYG)", "daily", "financial", "rates"),
    # ── International ETFs ──
    ("EWJ", "finnhub_ewj", "Finnhub: iShares Japan (EWJ)", "daily", "financial", "equity"),
    ("FXI", "finnhub_fxi", "Finnhub: iShares China (FXI)", "daily", "financial", "equity"),
    ("EWG", "finnhub_ewg", "Finnhub: iShares Germany (EWG)", "daily", "financial", "equity"),
    ("EWU", "finnhub_ewu", "Finnhub: iShares UK (EWU)", "daily", "financial", "equity"),
    ("EEM", "finnhub_eem", "Finnhub: iShares Emerging Markets (EEM)", "daily", "financial", "equity"),
    ("VEA", "finnhub_vea", "Finnhub: Vanguard Developed Markets (VEA)", "daily", "financial", "equity"),
]


def _make_finnhub_collector(
    symbol: str,
    name: str,
    display_name: str,
    frequency: str,
    domain: str,
    category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://finnhub.io/docs/api/stock-candles",
            requires_key=True,
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            resolution = "D"
            data = _fetch_candle(
                symbol, resolution, timeout=self.config.request_timeout,
            )

            status = data.get("s", "")
            if status == "no_data":
                raise RuntimeError(f"Finnhub: no data for {symbol}")
            if status != "ok":
                raise RuntimeError(f"Finnhub API error for {symbol}: {status}")

            timestamps = data.get("t", [])
            closes = data.get("c", [])
            if not timestamps or not closes:
                raise RuntimeError(f"Finnhub: empty candle data for {symbol}")

            rows: list[dict] = []
            for ts, close in zip(timestamps, closes):
                try:
                    dt = pd.to_datetime(ts, unit="s", utc=True)
                    rows.append({"date": dt, "value": float(close)})
                except (ValueError, TypeError):
                    continue

            if not rows:
                raise RuntimeError(f"No valid data for Finnhub {symbol}")

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Finnhub_{name}"
    _Collector.__qualname__ = f"Finnhub_{name}"
    return _Collector


def get_finnhub_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_finnhub_collector(*t) for t in FINNHUB_SERIES}
