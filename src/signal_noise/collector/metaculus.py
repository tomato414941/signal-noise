from __future__ import annotations

import os
import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


def _load_metaculus_token() -> str:
    token = os.environ.get("METACULUS_API_TOKEN", "")
    if not token:
        secrets = os.path.expanduser("~/.secrets/metaculus")
        if os.path.exists(secrets):
            for line in open(secrets):
                line = line.strip()
                if line.startswith("export METACULUS_API_TOKEN="):
                    token = line.split("=", 1)[1].strip("'\"")
    return token


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
        token = _load_metaculus_token()
        if not token:
            raise RuntimeError(
                "Metaculus API token not found. Set METACULUS_API_TOKEN env var "
                "or create ~/.secrets/metaculus"
            )
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
