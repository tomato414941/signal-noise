"""PeeringDB Internet infrastructure stats.

Tracks Internet Exchange Points (IXPs) count globally.
https://www.peeringdb.com/apidocs/
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE = "https://www.peeringdb.com/api"


class PeeringDBIXCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="peeringdb_ix_count",
        display_name="Internet Exchange Points (Global)",
        update_frequency="daily",
        api_docs_url="https://www.peeringdb.com/apidocs/",
        domain="technology",
        category="infra",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            f"{_BASE}/ix",
            params={"fields": "id", "depth": "0"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        count = len(resp.json().get("data", []))
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])
