from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class WHODiseaseOutbreakCollector(BaseCollector):
    """WHO Disease Outbreak News (DON) event count."""

    meta = CollectorMeta(
        name="who_disease_outbreaks",
        display_name="WHO Disease Outbreak Events",
        update_frequency="weekly",
        api_docs_url="https://www.who.int/emergencies/disease-outbreak-news",
        domain="health",
        category="epidemiology",
    )

    URL = "https://www.who.int/api/hubs/diseaseoutbreaknews?$orderby=PublicationDate desc&$top=500"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("value", [])
        if not data:
            raise RuntimeError("No WHO DON data")
        rows = []
        for entry in data:
            try:
                date_str = entry.get("PublicationDate", "")[:10]
                dt = pd.Timestamp(date_str, tz="UTC")
                rows.append({"date": dt.normalize()})
            except (ValueError, TypeError):
                continue
        df = pd.DataFrame(rows)
        monthly = df.groupby(df["date"].dt.to_period("M")).size().reset_index(name="value")
        monthly["date"] = monthly["date"].dt.to_timestamp(tz="UTC")
        return monthly.sort_values("date").reset_index(drop=True)
