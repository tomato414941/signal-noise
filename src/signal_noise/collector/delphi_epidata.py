"""CMU Delphi Epidata — flu hospitalizations and dengue cases.

Free API, no authentication required.
API: https://cmu-delphi.github.io/delphi-epidata/
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = "https://api.delphi.cmu.edu/epidata"


class DelphiFluHospitalizationsCollector(BaseCollector):
    """Weekly flu hospitalization rate (FluSurv-NET, all ages)."""

    meta = CollectorMeta(
        name="delphi_flu_hospitalizations",
        display_name="Flu Hospitalizations (Delphi)",
        update_frequency="weekly",
        api_docs_url="https://cmu-delphi.github.io/delphi-epidata/",
        domain="health",
        category="epidemiology",
    )

    def fetch(self) -> pd.DataFrame:
        now = pd.Timestamp.now(tz="UTC")
        year = now.year
        epiweeks = f"{year - 2}01-{year}52"
        resp = requests.get(
            f"{_BASE_URL}/flusurv/",
            params={"locations": "network_all", "epiweeks": epiweeks},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        epidata = data.get("epidata", [])
        if not epidata:
            return pd.DataFrame(columns=["date", "value"])
        rows = []
        for rec in epidata:
            ew = rec["epiweek"]
            yr, wk = int(str(ew)[:4]), int(str(ew)[4:])
            date = pd.Timestamp.fromisocalendar(yr, min(wk, 52), 1)
            rate = rec.get("rate_overall")
            if rate is None:
                continue
            rows.append({"date": date, "value": float(rate)})
        df = pd.DataFrame(rows)
        return df.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)


