"""Common Crawl index stats.

Tracks the total number of web crawl indexes published by
Common Crawl. Each index represents a snapshot of the open web.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_INDEX_URL = "https://index.commoncrawl.org/collinfo.json"


class CommonCrawlIndexCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="commoncrawl_index_count",
        display_name="Common Crawl Total Indexes",
        update_frequency="monthly",
        api_docs_url="https://commoncrawl.org/access-the-data",
        domain="technology",
        category="internet",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_INDEX_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        crawls = resp.json()
        if not crawls:
            raise RuntimeError("No Common Crawl index data")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(len(crawls))}])
