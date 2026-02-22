from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class AAIISentimentCollector(BaseCollector):
    """Crypto Fear & Greed Index via alternative.me (free, no key).

    Replaces AAII Investor Sentiment Survey which is blocked by WAF.
    The Fear & Greed index ranges from 0 (Extreme Fear) to 100 (Extreme Greed).
    """

    meta = CollectorMeta(
        name="aaii_bull_ratio",
        display_name="Crypto Fear & Greed Index",
        update_frequency="daily",
        api_docs_url="https://alternative.me/crypto/fear-and-greed-index/",
        domain="sentiment",
        category="sentiment",
    )

    URL = "https://api.alternative.me/fng/?limit=365&format=json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            raise RuntimeError("No Fear & Greed data from alternative.me")
        rows = []
        for entry in data:
            try:
                ts = int(entry["timestamp"])
                val = float(entry["value"])
                dt = pd.Timestamp(ts, unit="s", tz="UTC")
                rows.append({"date": dt, "value": val})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No parseable Fear & Greed data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
