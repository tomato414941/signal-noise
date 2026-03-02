from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CDCExcessDeathsCollector(BaseCollector):
    """CDC NCHS excess deaths — weekly US observed vs expected."""

    meta = CollectorMeta(
        name="cdc_excess_deaths",
        display_name="CDC US Weekly Excess Deaths",
        update_frequency="weekly",
        api_docs_url="https://data.cdc.gov/NCHS/Excess-Deaths-Associated-with-COVID-19/xkkf-xrst",
        domain="mortality",
        category="excess_deaths",
    )

    URL = (
        "https://data.cdc.gov/resource/xkkf-xrst.json"
        "?$where=state='United States' AND outcome='All causes'"
        "&$order=week_ending_date DESC"
        "&$limit=5000"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            raise RuntimeError("No CDC excess deaths data")
        rows = []
        for entry in data:
            try:
                dt = pd.to_datetime(entry["week_ending_date"], utc=True)
                observed = float(entry.get("observed_number", 0))
                expected = float(entry.get("expected", 0))
                excess = observed - expected if expected > 0 else 0
                rows.append({"date": dt, "value": excess})
            except (KeyError, ValueError, TypeError):
                continue
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
