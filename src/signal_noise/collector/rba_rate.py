from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class RBACashRateCollector(BaseCollector):
    """Reserve Bank of Australia lending rate via World Bank API.

    Uses FR.INR.LEND (Lending interest rate) as RBA's own API
    (api.data.rba.gov.au) is unreachable from many environments.
    """

    meta = CollectorMeta(
        name="rba_cash_rate",
        display_name="RBA Cash / Lending Rate (%)",
        update_frequency="yearly",
        api_docs_url="https://data.worldbank.org/indicator/FR.INR.LEND?locations=AU",
        domain="financial",
        category="rates",
    )

    URL = (
        "https://api.worldbank.org/v2/country/AUS/indicator/FR.INR.LEND"
        "?format=json&per_page=100&date=2000:2026"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2:
            raise RuntimeError("Unexpected World Bank response format")
        records = payload[1]
        if not records:
            raise RuntimeError("No World Bank data for AUS lending rate")
        rows = []
        for entry in records:
            val = entry.get("value")
            date_str = entry.get("date")
            if val is not None and date_str:
                try:
                    dt = pd.Timestamp(f"{date_str}-01-01", tz="UTC")
                    rows.append({"date": dt, "value": float(val)})
                except (ValueError, TypeError):
                    continue
        if not rows:
            raise RuntimeError("No parseable World Bank RBA data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
