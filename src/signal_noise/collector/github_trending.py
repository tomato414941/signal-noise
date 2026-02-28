"""GitHub Trending — L3 scraping collector.

Scrapes github.com/trending to extract daily trending repository metrics.
No official API exists for GitHub Trending — this is an L3 collector.

Snapshot signals — cannot be backfilled.
"""
from __future__ import annotations

import re

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_trending_cache = SharedAPICache(ttl=300)

_TRENDING_URL = "https://github.com/trending"
_HEADERS = {
    "User-Agent": "signal-noise/1.0 (github.com/tomato414941/signal-noise)",
    "Accept": "text/html",
}


def _fetch_trending() -> dict:
    """Scrape GitHub Trending page and extract metrics."""
    resp = requests.get(_TRENDING_URL, headers=_HEADERS, timeout=15)
    resp.raise_for_status()
    html = resp.text

    repo_count = len(re.findall(r'<article class="Box-row"', html))

    stars_today = re.findall(r'([\d,]+)\s+stars today', html)
    stars_today = [int(s.replace(",", "")) for s in stars_today]
    total_stars = sum(stars_today) if stars_today else 0

    languages = re.findall(r'itemprop="programmingLanguage">([^<]+)<', html)
    unique_langs = len(set(languages))

    return {
        "repo_count": repo_count,
        "total_stars_today": total_stars,
        "unique_languages": unique_langs,
    }


class GitHubTrendingReposCollector(BaseCollector):
    """Number of repositories on GitHub Trending page."""

    meta = CollectorMeta(
        name="github_trending_repos",
        display_name="GitHub Trending Repos",
        update_frequency="hourly",
        api_docs_url="",
        domain="developer",
        category="developer",
        collection_level="L3",
    )

    def fetch(self) -> pd.DataFrame:
        data = _trending_cache.get_or_fetch("trending", _fetch_trending)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(data["repo_count"])]})


class GitHubTrendingStarsCollector(BaseCollector):
    """Total stars gained today by GitHub Trending repositories."""

    meta = CollectorMeta(
        name="github_trending_stars_today",
        display_name="GitHub Trending Stars Today",
        update_frequency="hourly",
        api_docs_url="",
        domain="developer",
        category="developer",
        collection_level="L3",
    )

    def fetch(self) -> pd.DataFrame:
        data = _trending_cache.get_or_fetch("trending", _fetch_trending)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(data["total_stars_today"])]})


class GitHubTrendingLanguagesCollector(BaseCollector):
    """Number of distinct programming languages on GitHub Trending."""

    meta = CollectorMeta(
        name="github_trending_languages",
        display_name="GitHub Trending Languages",
        update_frequency="hourly",
        api_docs_url="",
        domain="developer",
        category="developer",
        collection_level="L3",
    )

    def fetch(self) -> pd.DataFrame:
        data = _trending_cache.get_or_fetch("trending", _fetch_trending)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(data["unique_languages"])]})
