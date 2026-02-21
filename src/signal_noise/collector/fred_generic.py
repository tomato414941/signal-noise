from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# FRED API key: set FRED_API_KEY env var or place in ~/.secrets/fred
_FRED_API_KEY: str | None = None


def _get_fred_key() -> str:
    global _FRED_API_KEY
    if _FRED_API_KEY:
        return _FRED_API_KEY

    key = os.environ.get("FRED_API_KEY")
    if not key:
        secret_path = os.path.expanduser("~/.secrets/fred")
        if os.path.exists(secret_path):
            with open(secret_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("export FRED_API_KEY="):
                        key = line.split("=", 1)[1].strip().strip("'\"")
                        break
    if not key:
        raise RuntimeError(
            "FRED_API_KEY not set. Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html"
        )
    _FRED_API_KEY = key
    return key


# (series_id, collector_name, display_name, data_type, frequency)
FRED_SERIES: list[tuple[str, str, str, str, str]] = [
    # ── Employment / labor ──
    ("ICSA", "fred_jobless_claims", "Initial Jobless Claims", "labor", "weekly"),
    ("UNRATE", "fred_unemployment", "US Unemployment Rate", "labor", "monthly"),
    ("PAYEMS", "fred_nonfarm_payrolls", "Nonfarm Payrolls", "labor", "monthly"),
    # ── Inflation / prices ──
    ("CPIAUCSL", "fred_cpi", "Consumer Price Index", "inflation", "monthly"),
    ("CPILFESL", "fred_core_cpi", "Core CPI (ex Food & Energy)", "inflation", "monthly"),
    ("PCEPI", "fred_pce", "PCE Price Index", "inflation", "monthly"),
    ("T10YIE", "fred_breakeven_10y", "10Y Breakeven Inflation", "inflation", "daily"),
    # ── Interest rates / monetary ──
    ("FEDFUNDS", "fred_fed_funds", "Federal Funds Rate", "monetary", "monthly"),
    ("DFF", "fred_daily_fed_funds", "Daily Fed Funds Rate", "monetary", "daily"),
    ("T10Y2Y", "fred_yield_spread", "10Y-2Y Yield Spread", "monetary", "daily"),
    ("T10Y3M", "fred_yield_spread_3m", "10Y-3M Yield Spread", "monetary", "daily"),
    # ── Money supply ──
    ("M2SL", "fred_m2", "M2 Money Supply", "monetary", "monthly"),
    ("WALCL", "fred_fed_balance", "Fed Balance Sheet Total", "monetary", "weekly"),
    # ── GDP / economic activity ──
    ("GDP", "fred_gdp", "US GDP", "economic", "quarterly"),
    ("GDPC1", "fred_real_gdp", "US Real GDP", "economic", "quarterly"),
    # ── Consumer / housing ──
    ("UMCSENT", "fred_consumer_sentiment", "U of Michigan Consumer Sentiment", "sentiment", "monthly"),
    ("HOUST", "fred_housing_starts", "Housing Starts", "housing", "monthly"),
    ("CSUSHPISA", "fred_case_shiller", "Case-Shiller Home Price Index", "housing", "monthly"),
    # ── Financial stress / volatility ──
    ("STLFSI2", "fred_financial_stress", "St. Louis Financial Stress Index", "stress", "weekly"),
    ("BAMLH0A0HYM2", "fred_high_yield_spread", "ICE BofA High Yield Spread", "stress", "daily"),
    # ── Trade / global ──
    ("DTWEXBGS", "fred_trade_weighted_usd", "Trade Weighted USD Index", "forex", "daily"),
    ("BOPGSTB", "fred_trade_balance", "US Trade Balance", "trade", "monthly"),
    # ── Commodities (via FRED) ──
    ("DCOILWTICO", "fred_wti_daily", "WTI Crude Oil (FRED daily)", "commodity", "daily"),
    ("GOLDAMGBD228NLBM", "fred_gold_daily", "Gold Price London Fix", "commodity", "daily"),
]


def _make_fred_collector(
    series_id: str, name: str, display_name: str, data_type: str, frequency: str
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = SourceMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            data_type=data_type,
            api_docs_url=f"https://fred.stlouisfed.org/series/{series_id}",
            requires_key=True,
        )

        def fetch(self) -> pd.DataFrame:
            api_key = _get_fred_key()
            end = datetime.now(UTC)
            start = end - timedelta(days=365 * 5)
            url = (
                f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id={series_id}"
                f"&api_key={api_key}"
                f"&file_type=json"
                f"&observation_start={start.strftime('%Y-%m-%d')}"
                f"&observation_end={end.strftime('%Y-%m-%d')}"
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            observations = resp.json().get("observations", [])

            rows = []
            for obs in observations:
                val = obs.get("value", ".")
                if val == ".":
                    continue
                rows.append({
                    "date": pd.to_datetime(obs["date"], utc=True),
                    "value": float(val),
                })

            if not rows:
                raise RuntimeError(f"No data for FRED series {series_id}")

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"FRED_{name}"
    _Collector.__qualname__ = f"FRED_{name}"
    return _Collector


def get_fred_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_fred_collector(*t) for t in FRED_SERIES}
