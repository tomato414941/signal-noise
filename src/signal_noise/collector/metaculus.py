from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector._auth import load_secret
from signal_noise.collector.base import BaseCollector, CollectorMeta


class MetaculusActiveQuestionsCollector(BaseCollector):
    """Metaculus — count of currently active (open) prediction questions."""

    meta = CollectorMeta(
        name="metaculus_active_questions",
        display_name="Metaculus Active Questions Count",
        update_frequency="daily",
        api_docs_url="https://www.metaculus.com/api/",
        requires_key=True,
        domain="sentiment",
        category="prediction_market",
    )

    URL = "https://www.metaculus.com/api/questions/?status=open&limit=1&offset=0"

    def fetch(self) -> pd.DataFrame:
        token = load_secret("metaculus", "METACULUS_API_TOKEN",
                            signup_url="https://www.metaculus.com/api/")
        headers = {
            "Authorization": f"Token {token}",
            "Accept": "application/json",
        }
        resp = requests.get(self.URL, timeout=self.config.request_timeout, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        count = data.get("count", 0)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": count}])
