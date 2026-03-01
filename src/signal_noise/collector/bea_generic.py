from __future__ import annotations

import os
import re

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_BEA_API_KEY: str | None = None
_BASE_URL = "https://apps.bea.gov/api/data"
_bea_cache = SharedAPICache(ttl=3600)

# Quarter string "2024Q1" -> pd.Timestamp
_Q_TO_MONTH = {"Q1": "01", "Q2": "04", "Q3": "07", "Q4": "10"}


def _get_bea_key() -> str:
    global _BEA_API_KEY
    if _BEA_API_KEY:
        return _BEA_API_KEY

    key = os.environ.get("BEA_API_KEY")
    if not key:
        secret_path = os.path.expanduser("~/.secrets/bea")
        if os.path.exists(secret_path):
            with open(secret_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("export BEA_API_KEY="):
                        key = line.split("=", 1)[1].strip().strip("'\"")
                        break
    if not key:
        raise RuntimeError(
            "BEA_API_KEY not set. Get a free key at https://apps.bea.gov/api/signup/"
        )
    _BEA_API_KEY = key
    return key


def _parse_period(period: str) -> pd.Timestamp | None:
    """Parse BEA TimePeriod like '2024Q1' or '2024' into a Timestamp."""
    m = re.match(r"^(\d{4})(Q[1-4])$", period)
    if m:
        year, q = m.groups()
        return pd.Timestamp(f"{year}-{_Q_TO_MONTH[q]}-01", tz="UTC")
    m = re.match(r"^(\d{4})$", period)
    if m:
        return pd.Timestamp(f"{period}-01-01", tz="UTC")
    return None


def _fetch_bea_table(
    dataset: str, table: str, frequency: str, timeout: int = 60,
) -> list[dict]:
    cache_key = f"{dataset}|{table}|{frequency}"

    def _fetch() -> list[dict]:
        api_key = _get_bea_key()
        freq_code = "Q" if frequency == "quarterly" else "A"
        params = {
            "UserID": api_key,
            "Method": "GetData",
            "DatasetName": dataset,
            "TableName": table,
            "Frequency": freq_code,
            "Year": "X",
            "ResultFormat": "JSON",
        }
        resp = requests.get(_BASE_URL, params=params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()

        results = payload.get("BEAAPI", {}).get("Results", {})
        if "Error" in results:
            msg = results["Error"].get("ErrorDetail", {}).get("Description", "unknown")
            raise RuntimeError(f"BEA API error: {msg}")

        return results.get("Data", [])

    return _bea_cache.get_or_fetch(cache_key, _fetch)


# (dataset, table, line_number, collector_name, display_name, frequency, domain, category)
BEA_SERIES: list[tuple[str, str, str, str, str, str, str, str]] = [
    # ── GDP Components (NIPA T10106 - Real GDP levels) ──
    ("NIPA", "T10106", "1", "bea_gdp_real", "BEA: Real GDP", "quarterly", "macro", "economic"),
    ("NIPA", "T10106", "2", "bea_pce_real", "BEA: Real PCE", "quarterly", "macro", "economic"),
    ("NIPA", "T10106", "7", "bea_fixed_investment", "BEA: Real Fixed Investment", "quarterly", "macro", "economic"),
    ("NIPA", "T10106", "11", "bea_exports_real", "BEA: Real Exports", "quarterly", "macro", "trade"),
    ("NIPA", "T10106", "12", "bea_imports_real", "BEA: Real Imports", "quarterly", "macro", "trade"),
    ("NIPA", "T10106", "15", "bea_govt_spending", "BEA: Real Govt Spending", "quarterly", "macro", "fiscal"),
    # ── GDP Growth (NIPA T10101 - Percent Change) ──
    ("NIPA", "T10101", "1", "bea_gdp_growth", "BEA: Real GDP Growth (%)", "quarterly", "macro", "economic"),
    # ── Personal Income (NIPA T20100) ──
    ("NIPA", "T20100", "1", "bea_personal_income", "BEA: Personal Income", "quarterly", "macro", "economic"),
    ("NIPA", "T20100", "27", "bea_disposable_income", "BEA: Disposable Personal Income", "quarterly", "macro", "economic"),
    ("NIPA", "T20100", "34", "bea_saving_rate", "BEA: Personal Saving Rate (%)", "quarterly", "macro", "economic"),
    # ── GDI (NIPA T11000 - Gross Domestic Income by Type of Income) ──
    ("NIPA", "T11000", "1", "bea_gdi", "BEA: Gross Domestic Income", "quarterly", "macro", "economic"),
    # ── Corporate Profits (NIPA T61600D - Corporate Profits by Industry) ──
    ("NIPA", "T61600D", "1", "bea_corporate_profits", "BEA: Corporate Profits", "quarterly", "macro", "economic"),
]


def _make_bea_collector(
    dataset: str,
    table: str,
    line_number: str,
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
            api_docs_url=f"https://apps.bea.gov/iTable/?reqid=19&step=2&isuri=1&categories=survey&nipa_table_list={table}",
            requires_key=True,
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            data = _fetch_bea_table(
                dataset, table, frequency,
                timeout=self.config.request_timeout,
            )

            rows: list[dict] = []
            for entry in data:
                if str(entry.get("LineNumber")) != line_number:
                    continue
                period = entry.get("TimePeriod", "")
                val_str = entry.get("DataValue", "")
                if not val_str or val_str == "---":
                    continue

                ts = _parse_period(period)
                if ts is None:
                    continue
                try:
                    val = float(val_str.replace(",", ""))
                    rows.append({"date": ts, "value": val})
                except (ValueError, TypeError):
                    continue

            if not rows:
                raise RuntimeError(f"No data for BEA {dataset}/{table} line {line_number}")

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"BEA_{name}"
    _Collector.__qualname__ = f"BEA_{name}"
    return _Collector


def get_bea_collectors() -> dict[str, type[BaseCollector]]:
    return {t[3]: _make_bea_collector(*t) for t in BEA_SERIES}
