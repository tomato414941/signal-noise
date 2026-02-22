from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

_USER_AGENT = "signal-noise/0.1 (https://github.com/tomato414941/signal-noise)"


class _RedditSubredditCollector(BaseCollector):
    """Base for Reddit subreddit activity.

    Uses the public JSON API (no OAuth required).
    Fetches /hot listing and extracts aggregate activity metrics.
    Data accumulates across collection runs.
    """

    _subreddit: str = ""

    def fetch(self) -> pd.DataFrame:
        # Fetch hot posts (up to 100)
        url = f"https://www.reddit.com/r/{self._subreddit}/hot.json?limit=100"
        resp = requests.get(
            url,
            headers={"User-Agent": _USER_AGENT},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json()

        posts = data.get("data", {}).get("children", [])
        if not posts:
            raise RuntimeError(f"No posts from r/{self._subreddit}")

        total_score = 0
        total_comments = 0
        for post in posts:
            d = post.get("data", {})
            total_score += d.get("score", 0)
            total_comments += d.get("num_comments", 0)

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        # Activity index = total score + comments across top 100 hot posts
        activity = float(total_score + total_comments)

        return pd.DataFrame({"timestamp": [ts], "value": [activity]})


class RedditCryptoCollector(_RedditSubredditCollector):
    _subreddit = "cryptocurrency"
    meta = SourceMeta(
        name="reddit_crypto",
        display_name="Reddit r/cryptocurrency Activity",
        update_frequency="hourly",
        api_docs_url="https://www.reddit.com/dev/api/",
        domain="sentiment",
        category="attention",
    )


class RedditWsbCollector(_RedditSubredditCollector):
    _subreddit = "wallstreetbets"
    meta = SourceMeta(
        name="reddit_wsb",
        display_name="Reddit r/wallstreetbets Activity",
        update_frequency="hourly",
        api_docs_url="https://www.reddit.com/dev/api/",
        domain="sentiment",
        category="attention",
    )


class RedditBitcoinCollector(_RedditSubredditCollector):
    _subreddit = "Bitcoin"
    meta = SourceMeta(
        name="reddit_bitcoin",
        display_name="Reddit r/Bitcoin Activity",
        update_frequency="hourly",
        api_docs_url="https://www.reddit.com/dev/api/",
        domain="sentiment",
        category="attention",
    )
