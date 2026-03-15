"""PeerTube instance count via joinpeertube.org API.

Tracks the growth of the PeerTube federated video network.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class PeerTubeInstancesCollector(BaseCollector):
    meta = CollectorMeta(
        name="peertube_instances",
        display_name="PeerTube Instances (Total)",
        update_frequency="daily",
        api_docs_url="https://instances.joinpeertube.org/",
        domain="technology",
        category="internet",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            "https://instances.joinpeertube.org/api/v1/instances",
            params={"count": "1"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        total = resp.json().get("total", 0)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])
