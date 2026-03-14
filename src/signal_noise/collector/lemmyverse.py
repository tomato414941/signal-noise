"""Lemmyverse (Lemmy federation) instance stats.

Tracks the number of Lemmy instances in the fediverse.
Combined with FediDB, provides a view of decentralized
social platform fragmentation vs consolidation.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://data.lemmyverse.net/data/instance.full.json"


class LemmyInstanceCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="lemmy_instances",
        display_name="Lemmy Instances",
        update_frequency="daily",
        api_docs_url="https://lemmyverse.net/",
        domain="sentiment",
        category="attention",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_API_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        instances = resp.json()
        if not isinstance(instances, list):
            raise RuntimeError("Unexpected Lemmyverse response")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(len(instances))}])


class LemmyTotalUsersCollector(BaseCollector):
    meta = CollectorMeta(
        name="lemmy_total_users",
        display_name="Lemmy Total Users",
        update_frequency="daily",
        api_docs_url="https://lemmyverse.net/",
        domain="sentiment",
        category="attention",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_API_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        instances = resp.json()
        if not isinstance(instances, list):
            raise RuntimeError("Unexpected Lemmyverse response")
        total = sum(i.get("counts", {}).get("users", 0) for i in instances)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])
