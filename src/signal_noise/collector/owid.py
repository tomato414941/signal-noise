from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class OWIDCovidCollector(BaseCollector):
    """Our World in Data global COVID-19 new cases (daily)."""

    meta = CollectorMeta(
        name="owid_covid_cases",
        display_name="OWID Global COVID-19 New Cases",
        update_frequency="daily",
        api_docs_url="https://github.com/owid/covid-19-data",
        domain="society",
        category="epidemiology",
    )

    URL = (
        "https://raw.githubusercontent.com/owid/covid-19-data"
        "/master/public/data/jhu/new_cases.csv"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        from io import StringIO
        raw = pd.read_csv(StringIO(resp.text))
        if "World" not in raw.columns:
            raise RuntimeError("No World column in OWID data")
        df = raw[["date", "World"]].dropna().copy()
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"], utc=True)
        return df.sort_values("date").reset_index(drop=True)
