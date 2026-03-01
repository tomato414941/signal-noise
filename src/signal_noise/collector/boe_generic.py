from __future__ import annotations

from datetime import UTC, datetime, timedelta
from io import StringIO

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (series_code, collector_name, display_name, frequency, domain, category)
BOE_SERIES: list[tuple[str, str, str, str, str, str]] = [
    # ── Policy rate ──
    ("IUDBEDR", "boe_bank_rate", "BOE: Bank Rate", "daily", "financial", "rates"),
    ("IUMABEDR", "boe_bank_rate_monthly", "BOE: Bank Rate (Monthly Avg)", "monthly", "financial", "rates"),
    # ── Gilt yields ──
    ("IUMALNPY", "boe_long_yield", "BOE: Long Nominal Par Yield", "monthly", "financial", "rates"),
    ("IUDSNPY", "boe_short_yield", "BOE: Short Nominal Par Yield (Daily)", "daily", "financial", "rates"),
    ("IUMSNPY", "boe_short_yield_monthly", "BOE: Short Nominal Par Yield (Monthly)", "monthly", "financial", "rates"),
]

_BOE_URL = (
    "https://www.bankofengland.co.uk/boeapps/database/"
    "_iadb-FromShowColumns.asp"
)


def _make_boe_collector(
    series_code: str,
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
            api_docs_url="https://www.bankofengland.co.uk/boeapps/database/",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=365 * 5)
            params = {
                "csv.x": "yes",
                "Datefrom": start.strftime("%d/%b/%Y"),
                "Dateto": end.strftime("%d/%b/%Y"),
                "SeriesCodes": series_code,
                "CSVF": "TN",
                "UsingCodes": "Y",
                "VPD": "Y",
                "VFD": "N",
            }
            resp = requests.get(
                _BOE_URL, params=params, timeout=60,
                headers={"User-Agent": "signal-noise/1.0"},
            )
            resp.raise_for_status()

            text = resp.text.strip()
            if text.startswith("<!") or text.startswith("<html"):
                raise RuntimeError(f"BOE returned HTML instead of CSV for {series_code}")

            raw = pd.read_csv(StringIO(text))
            if raw.shape[1] < 2:
                raise RuntimeError(f"Unexpected BOE CSV format for {series_code}")

            df = raw.iloc[:, :2].copy()
            df.columns = ["date", "value"]
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["value"])
            df["date"] = pd.to_datetime(df["date"], format="%d %b %Y", utc=True)

            if df.empty:
                raise RuntimeError(f"No data for BOE series {series_code}")

            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"BOE_{name}"
    _Collector.__qualname__ = f"BOE_{name}"
    return _Collector


def get_boe_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_boe_collector(*t) for t in BOE_SERIES}
