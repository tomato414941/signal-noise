from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class MetaculusActiveQuestionsCollector(BaseCollector):
    """Metaculus — count of currently active (open) prediction questions."""

    meta = CollectorMeta(
        name="metaculus_active_questions",
        display_name="Metaculus Active Questions Count",
        update_frequency="daily",
        api_docs_url="https://www.metaculus.com/api/",
        domain="prediction",
        category="prediction_market",
    )

    URL = "https://www.metaculus.com/api2/questions/?status=open&limit=1&offset=0"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            self.URL,
            timeout=self.config.request_timeout,
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
        count = data.get("count", 0)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": count}])
