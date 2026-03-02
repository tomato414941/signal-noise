"""GitHub Trending — L3 scraping collector.

Scrapes github.com/trending to extract daily trending repository metrics.
No official API exists for GitHub Trending — this is an L3 collector.

Snapshot signals — cannot be backfilled.
"""
from __future__ import annotations

import logging
from html.parser import HTMLParser

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

log = logging.getLogger(__name__)

_trending_cache = SharedAPICache(ttl=300)

_TRENDING_URL = "https://github.com/trending"
_HEADERS = {
    "User-Agent": "signal-noise/1.0 (github.com/tomato414941/signal-noise)",
    "Accept": "text/html",
}


class _TrendingParser(HTMLParser):
    """Extract metrics from GitHub Trending HTML using standard html.parser."""

    def __init__(self) -> None:
        super().__init__()
        self.repo_count = 0
        self.stars_today: list[int] = []
        self.languages: list[str] = []
        self._in_stars_span = False
        self._in_lang = False
        self._current_text = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_dict = dict(attrs)
        # Count article.Box-row elements (each is a trending repo)
        if tag == "article":
            classes = (attr_dict.get("class") or "").split()
            if "Box-row" in classes:
                self.repo_count += 1
        # Detect language span
        if tag == "span" and attr_dict.get("itemprop") == "programmingLanguage":
            self._in_lang = True
            self._current_text = ""
        # Detect stars-today text (inside inline svg sibling spans)
        if tag == "span" and "float-sm-right" in (attr_dict.get("class") or ""):
            self._in_stars_span = True
            self._current_text = ""

    def handle_data(self, data: str) -> None:
        if self._in_lang:
            self._current_text += data
        if self._in_stars_span:
            self._current_text += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "span" and self._in_lang:
            self._in_lang = False
            lang = self._current_text.strip()
            if lang:
                self.languages.append(lang)
        if tag == "span" and self._in_stars_span:
            self._in_stars_span = False
            text = self._current_text.strip()
            if "stars today" in text:
                num_str = text.split("stars")[0].strip().replace(",", "")
                try:
                    self.stars_today.append(int(num_str))
                except ValueError:
                    pass


def _fetch_trending() -> dict:
    """Scrape GitHub Trending page and extract metrics."""
    resp = requests.get(_TRENDING_URL, headers=_HEADERS, timeout=15)
    resp.raise_for_status()

    parser = _TrendingParser()
    parser.feed(resp.text)

    if parser.repo_count == 0:
        log.warning("GitHub Trending: parsed 0 repos — page structure may have changed")

    return {
        "repo_count": parser.repo_count,
        "total_stars_today": sum(parser.stars_today),
        "unique_languages": len(set(parser.languages)),
    }


class GitHubTrendingReposCollector(BaseCollector):
    """Number of repositories on GitHub Trending page."""

    meta = CollectorMeta(
        name="github_trending_repos",
        display_name="GitHub Trending Repos",
        update_frequency="hourly",
        api_docs_url="",
        domain="technology",
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
        domain="technology",
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
        domain="technology",
        category="developer",
        collection_level="L3",
    )

    def fetch(self) -> pd.DataFrame:
        data = _trending_cache.get_or_fetch("trending", _fetch_trending)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(data["unique_languages"])]})
