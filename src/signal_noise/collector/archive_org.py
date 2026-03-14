"""Internet Archive (archive.org) collection stats.

Tracks total item count in the Internet Archive. Growth reflects
digital preservation efforts and web archiving velocity.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_SEARCH_URL = "https://archive.org/advancedsearch.php"


class ArchiveOrgTotalItemsCollector(BaseCollector):
    meta = CollectorMeta(
        name="archive_org_total_items",
        display_name="Internet Archive Total Items",
        update_frequency="daily",
        api_docs_url="https://archive.org/developers/",
        domain="technology",
        category="internet",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            _SEARCH_URL,
            params={"q": "*", "rows": "0", "output": "json"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        count = resp.json().get("response", {}).get("numFound")
        if count is None:
            raise RuntimeError("No Archive.org item count")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])
