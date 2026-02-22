from __future__ import annotations

import logging

import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

log = logging.getLogger(__name__)


class GoogleTrendsCollector(BaseCollector):
    meta = SourceMeta(
        name="google_trends",
        display_name="Google Trends (bitcoin)",
        update_frequency="weekly",
        data_type="attention",
        api_docs_url="https://trends.google.com/trends/",
        domain="sentiment",
        category="attention",
    )

    def fetch(self) -> pd.DataFrame:
        try:
            from pytrends.request import TrendReq
        except ImportError:
            raise RuntimeError(
                "pytrends not installed. Install with: pip install 'signal-noise[trends]'"
            )

        pytrends = TrendReq(hl="en-US", tz=0)
        pytrends.build_payload(["bitcoin"], timeframe="today 5-y")
        interest = pytrends.interest_over_time()

        if interest.empty:
            raise RuntimeError("No Google Trends data returned")

        interest = interest.drop(columns=["isPartial"], errors="ignore")
        df = pd.DataFrame({
            "date": interest.index.tz_localize("UTC") if interest.index.tz is None else interest.index,
            "value": interest["bitcoin"].values,
        })
        df = df.sort_values("date").reset_index(drop=True)
        return df
