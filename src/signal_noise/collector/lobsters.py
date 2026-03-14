"""Lobste.rs tech news aggregator collectors.

Tracks daily story metrics and AI-topic share from the invite-only
tech community. Higher-signal alternative to HN for developer
attention trends.
"""
from __future__ import annotations

import time

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_HOTTEST_URL = "https://lobste.rs/hottest.json"
_NEWEST_URL = "https://lobste.rs/newest.json"

_cache: list | None = None
_cache_ts: float = 0.0


def _fetch_hottest(timeout: int = 30) -> list[dict]:
    global _cache, _cache_ts
    now = time.monotonic()
    if _cache is not None and (now - _cache_ts) < 600:
        return _cache
    resp = requests.get(_HOTTEST_URL, timeout=timeout)
    resp.raise_for_status()
    _cache = resp.json()
    _cache_ts = now
    return _cache


class LobstersAvgScoreCollector(BaseCollector):
    meta = CollectorMeta(
        name="lobsters_avg_score",
        display_name="Lobste.rs Hottest Avg Score",
        update_frequency="daily",
        api_docs_url="https://lobste.rs/",
        domain="sentiment",
        category="attention",
    )

    def fetch(self) -> pd.DataFrame:
        stories = _fetch_hottest(timeout=self.config.request_timeout)
        if not stories:
            raise RuntimeError("No Lobste.rs stories")
        avg = sum(s.get("score", 0) for s in stories) / len(stories)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": round(avg, 2)}])


class LobstersAvgCommentsCollector(BaseCollector):
    meta = CollectorMeta(
        name="lobsters_avg_comments",
        display_name="Lobste.rs Hottest Avg Comments",
        update_frequency="daily",
        api_docs_url="https://lobste.rs/",
        domain="sentiment",
        category="attention",
    )

    def fetch(self) -> pd.DataFrame:
        stories = _fetch_hottest(timeout=self.config.request_timeout)
        if not stories:
            raise RuntimeError("No Lobste.rs stories")
        avg = sum(s.get("comment_count", 0) for s in stories) / len(stories)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": round(avg, 2)}])


class LobstersAIShareCollector(BaseCollector):
    """Fraction of hottest stories tagged with AI-related topics."""

    meta = CollectorMeta(
        name="lobsters_ai_share",
        display_name="Lobste.rs AI Topic Share",
        update_frequency="daily",
        api_docs_url="https://lobste.rs/",
        domain="sentiment",
        category="attention",
    )

    _AI_TAGS = {"ai", "ml", "machine-learning", "llm", "chatgpt", "generative-ai"}

    def fetch(self) -> pd.DataFrame:
        stories = _fetch_hottest(timeout=self.config.request_timeout)
        if not stories:
            raise RuntimeError("No Lobste.rs stories")
        ai_count = sum(
            1 for s in stories
            if any(t in self._AI_TAGS for t in s.get("tags", []))
        )
        share = ai_count / len(stories)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": round(share, 4)}])
