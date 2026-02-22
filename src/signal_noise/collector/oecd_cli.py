from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class OECDCLICollector(BaseCollector):
    """OECD Composite Leading Indicator (OECD total area)."""

    meta = CollectorMeta(
        name="oecd_cli",
        display_name="OECD Composite Leading Indicator",
        update_frequency="monthly",
        api_docs_url="https://data.oecd.org/leadind/composite-leading-indicator-cli.htm",
        domain="macro",
        category="economic",
    )

    URL = (
        "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_KEI@DF_KEI,4.0/"
        "M.OECD.LI.LOLITOAA.IXOBSA..?startPeriod=2000-01&dimensionAtObservation=AllDimensions"
        "&format=jsondata"
    )

    def fetch(self) -> pd.DataFrame:
        headers = {"Accept": "application/json"}
        resp = requests.get(self.URL, headers=headers, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        # SDMX-JSON structure
        obs = data.get("dataSets", [{}])[0].get("observations", {})
        dims = data.get("structure", {}).get("dimensions", {}).get("observation", [])
        time_dim = next((d for d in dims if d["id"] == "TIME_PERIOD"), None)
        if not time_dim or not obs:
            raise RuntimeError("No OECD CLI data")
        time_values = {str(i): v["id"] for i, v in enumerate(time_dim["values"])}
        rows = []
        for key, val in obs.items():
            parts = key.split(":")
            time_idx = parts[-1] if len(parts) > 0 else ""
            period = time_values.get(time_idx, "")
            if period and val:
                try:
                    dt = pd.Timestamp(period + "-01", tz="UTC")
                    rows.append({"date": dt, "value": float(val[0])})
                except (ValueError, TypeError, IndexError):
                    continue
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
