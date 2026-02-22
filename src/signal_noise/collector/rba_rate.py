from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class RBACashRateCollector(BaseCollector):
    """Reserve Bank of Australia cash rate target."""

    meta = CollectorMeta(
        name="rba_cash_rate",
        display_name="RBA Cash Rate Target (%)",
        update_frequency="monthly",
        api_docs_url="https://www.rba.gov.au/statistics/tables/",
        domain="financial",
        category="rates",
    )

    URL = "https://api.data.rba.gov.au/datafiles/latest/cash-rate-target.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        series = data.get("data", data.get("observations", []))
        if not series:
            raise RuntimeError("No RBA data")
        rows = []
        for entry in series:
            try:
                dt = pd.Timestamp(entry.get("date") or entry.get("d"), tz="UTC")
                val = float(entry.get("value") or entry.get("v"))
                rows.append({"date": dt, "value": val})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No parseable RBA data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
