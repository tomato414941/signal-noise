"""ECB (European Central Bank) daily reference exchange rates.

No API key required.  Uses the ECB Statistical Data Warehouse API.
Docs: https://data.ecb.europa.eu/help/api/data
"""
from __future__ import annotations

from io import StringIO

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._utils import build_timeseries_df

_BASE_URL = "https://data-api.ecb.europa.eu/service/data/EXR"

# (currency, collector_name, display_name)
_FX_SERIES: list[tuple[str, str, str]] = [
    ("USD", "ecb_fx_usd", "ECB: EUR/USD"),
    ("JPY", "ecb_fx_jpy", "ECB: EUR/JPY"),
    ("GBP", "ecb_fx_gbp", "ECB: EUR/GBP"),
    ("CHF", "ecb_fx_chf", "ECB: EUR/CHF"),
    ("CNY", "ecb_fx_cny", "ECB: EUR/CNY"),
    ("AUD", "ecb_fx_aud", "ECB: EUR/AUD"),
    ("CAD", "ecb_fx_cad", "ECB: EUR/CAD"),
    ("SEK", "ecb_fx_sek", "ECB: EUR/SEK"),
    ("NOK", "ecb_fx_nok", "ECB: EUR/NOK"),
    ("PLN", "ecb_fx_pln", "ECB: EUR/PLN"),
    ("TRY", "ecb_fx_try", "ECB: EUR/TRY"),
    ("BRL", "ecb_fx_brl", "ECB: EUR/BRL"),
    ("INR", "ecb_fx_inr", "ECB: EUR/INR"),
    ("KRW", "ecb_fx_krw", "ECB: EUR/KRW"),
    ("MXN", "ecb_fx_mxn", "ECB: EUR/MXN"),
    ("ZAR", "ecb_fx_zar", "ECB: EUR/ZAR"),
]


def _make_ecb_fx_collector(
    currency: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://data.ecb.europa.eu/help/api/data",
            domain="economy",
            category="economic",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"{_BASE_URL}/D.{currency}.EUR.SP00.A"
            resp = requests.get(
                url,
                params={"format": "csvdata", "startPeriod": "2020-01-01"},
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            raw = pd.read_csv(StringIO(resp.text))
            if "TIME_PERIOD" not in raw.columns or "OBS_VALUE" not in raw.columns:
                raise RuntimeError(f"ECB: unexpected CSV columns for {currency}")
            df = raw[["TIME_PERIOD", "OBS_VALUE"]].dropna().copy()
            df.columns = ["date", "value"]
            df["date"] = pd.to_datetime(df["date"], utc=True)
            df["value"] = df["value"].astype(float)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"ECB_FX_{currency}"
    _Collector.__qualname__ = f"ECB_FX_{currency}"
    return _Collector


def get_ecb_fx_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_ecb_fx_collector(currency, name, display_name)
        for currency, name, display_name in _FX_SERIES
    }
