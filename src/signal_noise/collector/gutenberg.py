"""Project Gutenberg book count via Gutendex API.

Tracks total number of free ebooks available.
https://gutendex.com/
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class GutenbergBookCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="gutenberg_book_count",
        display_name="Project Gutenberg Books (Total)",
        update_frequency="daily",
        api_docs_url="https://gutendex.com/",
        domain="society",
        category="culture",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            "https://gutendex.com/books/?page=1",
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        count = resp.json().get("count", 0)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])
