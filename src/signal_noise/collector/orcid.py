"""ORCID researcher registry stats.

Tracks total registered researchers on the ORCID platform.
Growth reflects academic community digitization and open
research infrastructure adoption.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://pub.orcid.org/v3.0/search/"


class ORCIDResearcherCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="orcid_researchers",
        display_name="ORCID Registered Researchers",
        update_frequency="daily",
        api_docs_url="https://info.orcid.org/documentation/api-tutorials/",
        domain="technology",
        category="academic",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            _API_URL,
            params={"q": "*", "rows": "0"},
            headers={"Accept": "application/json"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        count = resp.json().get("num-found")
        if count is None:
            raise RuntimeError("No ORCID researcher count")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])
