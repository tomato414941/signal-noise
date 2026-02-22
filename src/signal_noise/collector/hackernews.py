from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

_HN_BASE = "https://hacker-news.firebaseio.com/v0"


class _HNListCollector(BaseCollector):
    """Base for HN list-based collectors (top, best, new).

    Fetches story IDs, samples N items, computes aggregate score.
    """

    _endpoint: str = ""
    _sample_size: int = 30

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            f"{_HN_BASE}/{self._endpoint}.json",
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        story_ids = resp.json()[:self._sample_size]

        total_score = 0
        total_comments = 0
        for sid in story_ids:
            try:
                item_resp = requests.get(
                    f"{_HN_BASE}/item/{sid}.json",
                    timeout=self.config.request_timeout,
                )
                item_resp.raise_for_status()
                item = item_resp.json()
                total_score += item.get("score", 0)
                total_comments += item.get("descendants", 0)
            except Exception:
                continue

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({
            "timestamp": [ts],
            "value": [float(total_score + total_comments)],
        })


class HNTopCollector(_HNListCollector):
    _endpoint = "topstories"
    meta = SourceMeta(
        name="hn_top",
        display_name="Hacker News Top Stories Activity",
        update_frequency="hourly",
        data_type="tech_attention",
        api_docs_url="https://github.com/HackerNews/API",
        domain="sentiment",
        category="attention",
    )


class HNBestCollector(_HNListCollector):
    _endpoint = "beststories"
    meta = SourceMeta(
        name="hn_best",
        display_name="Hacker News Best Stories Activity",
        update_frequency="hourly",
        data_type="tech_attention",
        api_docs_url="https://github.com/HackerNews/API",
        domain="sentiment",
        category="attention",
    )


class HNNewCollector(_HNListCollector):
    _endpoint = "newstories"
    _sample_size = 50
    meta = SourceMeta(
        name="hn_new",
        display_name="Hacker News New Stories Activity",
        update_frequency="hourly",
        data_type="tech_attention",
        api_docs_url="https://github.com/HackerNews/API",
        domain="sentiment",
        category="attention",
    )
