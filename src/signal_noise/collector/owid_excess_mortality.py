from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class OWIDExcessMortalityCollector(BaseCollector):
    """Our World in Data global excess mortality (p-scores)."""

    meta = CollectorMeta(
        name="owid_excess_mortality",
        display_name="OWID Global Excess Mortality (P-score)",
        update_frequency="weekly",
        api_docs_url="https://github.com/owid/covid-19-data/tree/master/public/data/excess_mortality",
        domain="mortality",
        category="excess_deaths",
    )

    URL = (
        "https://raw.githubusercontent.com/owid/covid-19-data"
        "/master/public/data/excess_mortality/excess_mortality.csv"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        from io import StringIO
        raw = pd.read_csv(StringIO(resp.text))
        subset = pd.DataFrame()
        for loc in ("World", "High-income countries", "United States"):
            subset = raw[raw["location"] == loc].copy()
            if not subset.empty:
                break
        if subset.empty:
            raise RuntimeError("No aggregate excess mortality data found")
        df = subset[["date", "p_scores_all_ages"]].dropna().copy()
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"], utc=True)
        return df.sort_values("date").reset_index(drop=True)
