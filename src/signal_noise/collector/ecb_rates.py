from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class ECBDepositRateCollector(BaseCollector):
    """ECB deposit facility rate (daily)."""

    meta = CollectorMeta(
        name="ecb_deposit_rate",
        display_name="ECB Deposit Facility Rate (%)",
        update_frequency="daily",
        api_docs_url="https://data.ecb.europa.eu/",
        domain="financial",
        category="rates",
    )

    URL = (
        "https://data-api.ecb.europa.eu/service/data/FM/D.U2.EUR.4F.KR.DFR.LEV"
        "?format=jsondata&startPeriod=2000-01-01"
    )

    def fetch(self) -> pd.DataFrame:
        headers = {"Accept": "application/json"}
        resp = requests.get(self.URL, headers=headers, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        obs = data.get("dataSets", [{}])[0].get("series", {})
        # Find the first series
        series_key = next(iter(obs), None)
        if not series_key:
            raise RuntimeError("No ECB rate data")
        observations = obs[series_key].get("observations", {})
        dims = data.get("structure", {}).get("dimensions", {}).get("observation", [])
        time_dim = next((d for d in dims if d["id"] == "TIME_PERIOD"), None)
        if not time_dim:
            raise RuntimeError("No time dimension in ECB data")
        time_values = {str(i): v["id"] for i, v in enumerate(time_dim["values"])}
        rows = []
        for idx, val in observations.items():
            period = time_values.get(idx, "")
            if period and val:
                try:
                    dt = pd.Timestamp(period, tz="UTC")
                    rows.append({"date": dt, "value": float(val[0])})
                except (ValueError, TypeError, IndexError):
                    continue
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
