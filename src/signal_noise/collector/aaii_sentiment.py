from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class AAIISentimentCollector(BaseCollector):
    """AAII Investor Sentiment Survey — bullish percentage."""

    meta = CollectorMeta(
        name="aaii_bull_ratio",
        display_name="AAII Bullish Sentiment %",
        update_frequency="weekly",
        api_docs_url="https://www.aaii.com/sentimentsurvey",
        domain="sentiment",
        category="sentiment",
    )

    URL = "https://www.aaii.com/files/surveys/sentiment.xls"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        from io import BytesIO
        raw = pd.read_excel(BytesIO(resp.content), sheet_name=0, skiprows=3)
        # Columns: Date, Bullish, Neutral, Bearish
        if raw.shape[1] < 2:
            raise RuntimeError("Unexpected AAII format")
        df = raw.iloc[:, :2].dropna().copy()
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df["value"] = df["value"].astype(float) * 100  # fraction -> percent
        return df.sort_values("date").reset_index(drop=True)
