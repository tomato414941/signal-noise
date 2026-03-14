"""UK Parliament petitions collector.

Tracks the number of open petitions on the UK Parliament
website. Petition volume reflects public policy engagement
and civic activism levels.
"""
from __future__ import annotations

import re

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://petition.parliament.uk/petitions.json"


class UKOpenPetitionsCollector(BaseCollector):
    meta = CollectorMeta(
        name="uk_open_petitions",
        display_name="UK Parliament Open Petitions",
        update_frequency="daily",
        api_docs_url="https://petition.parliament.uk/help",
        domain="society",
        category="governance",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            _API_URL,
            params={"state": "open", "page": "1"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        last_link = data.get("links", {}).get("last", "")
        per_page = len(data.get("data", []))
        m = re.search(r"page=(\d+)", last_link)
        total_pages = int(m.group(1)) if m else 1
        total = total_pages * per_page
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])
