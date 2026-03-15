"""Apple App Store ranking collectors.

Tracks the position of key apps in the US top-free charts as a proxy
for consumer tech adoption and AI competition.

No API key required.  Uses Apple's public RSS feed.
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_FEED_URL = "https://rss.marketingtools.apple.com/api/v2/us/apps/top-free/100/apps.json"

# (app_name_substring, collector_name, display_name)
_APPS: list[tuple[str, str, str]] = [
    ("ChatGPT", "appstore_chatgpt", "App Store Rank: ChatGPT"),
    ("Claude by Anthropic", "appstore_claude", "App Store Rank: Claude"),
    ("Google Gemini", "appstore_gemini", "App Store Rank: Gemini"),
    ("Grok", "appstore_grok", "App Store Rank: Grok"),
    ("Perplexity", "appstore_perplexity", "App Store Rank: Perplexity"),
    ("Microsoft Copilot", "appstore_copilot", "App Store Rank: Copilot"),
    ("TikTok", "appstore_tiktok", "App Store Rank: TikTok"),
    ("Instagram", "appstore_instagram", "App Store Rank: Instagram"),
    ("Threads", "appstore_threads", "App Store Rank: Threads"),
    ("WhatsApp", "appstore_whatsapp", "App Store Rank: WhatsApp"),
    ("Telegram", "appstore_telegram", "App Store Rank: Telegram"),
    ("Spotify", "appstore_spotify", "App Store Rank: Spotify"),
    ("YouTube", "appstore_youtube", "App Store Rank: YouTube"),
    ("Snapchat", "appstore_snapchat", "App Store Rank: Snapchat"),
    ("X", "appstore_x", "App Store Rank: X"),
]

_cache: dict | None = None
_cache_ts: float = 0.0


def _fetch_rankings(timeout: int = 30) -> list[dict]:
    global _cache, _cache_ts
    import time
    now = time.monotonic()
    if _cache is not None and (now - _cache_ts) < 600:
        return _cache
    resp = requests.get(_FEED_URL, timeout=timeout)
    resp.raise_for_status()
    results = resp.json().get("feed", {}).get("results", [])
    _cache = results
    _cache_ts = now
    return results


def _make_appstore_rank_collector(
    app_substr: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://rss.marketingtools.apple.com/",
            domain="sentiment",
            category="attention",
        )

        def fetch(self) -> pd.DataFrame:
            results = _fetch_rankings(timeout=self.config.request_timeout)
            rank = 0.0
            for i, r in enumerate(results):
                if app_substr.lower() in r.get("name", "").lower():
                    rank = float(i + 1)
                    break
            now = pd.Timestamp.now(tz="UTC").floor("D")
            return pd.DataFrame([{"date": now, "value": rank}])

    _Collector.__name__ = f"AppStore_{name}"
    _Collector.__qualname__ = f"AppStore_{name}"
    return _Collector


def get_appstore_rank_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_appstore_rank_collector(app, name, display)
        for app, name, display in _APPS
    }
