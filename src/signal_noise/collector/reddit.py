from __future__ import annotations

import logging
import time

import requests
import pandas as pd

from signal_noise.collector._auth import load_secrets
from signal_noise.collector.base import BaseCollector, CollectorMeta

log = logging.getLogger(__name__)

_USER_AGENT = "signal-noise/0.1 (https://github.com/tomato414941/signal-noise)"

_token: str | None = None
_token_expires: float = 0.0


def _get_reddit_credentials() -> tuple[str, str]:
    creds = load_secrets("reddit", ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET"],
                         signup_url="https://www.reddit.com/prefs/apps")
    return creds["REDDIT_CLIENT_ID"], creds["REDDIT_CLIENT_SECRET"]


def _get_oauth_token() -> str:
    global _token, _token_expires
    if _token and time.time() < _token_expires:
        return _token

    cid, secret = _get_reddit_credentials()
    resp = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=(cid, secret),
        data={"grant_type": "client_credentials"},
        headers={"User-Agent": _USER_AGENT},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    _token = data["access_token"]
    # Expire 60s early to avoid edge cases
    _token_expires = time.time() + data.get("expires_in", 3600) - 60
    log.info("Reddit OAuth token acquired (expires in %ds)", data.get("expires_in", 3600))
    return _token


class _RedditSubredditCollector(BaseCollector):
    """Base for Reddit subreddit activity.

    Uses Reddit OAuth (client_credentials) to access the API.
    Fetches /hot listing and extracts aggregate activity metrics.
    Data accumulates across collection runs.
    """

    _subreddit: str = ""

    def fetch(self) -> pd.DataFrame:
        token = _get_oauth_token()
        url = f"https://oauth.reddit.com/r/{self._subreddit}/hot?limit=100"
        resp = requests.get(
            url,
            headers={
                "User-Agent": _USER_AGENT,
                "Authorization": f"Bearer {token}",
            },
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
        activity = float(total_score + total_comments)

        return pd.DataFrame({"timestamp": [ts], "value": [activity]})


class RedditCryptoCollector(_RedditSubredditCollector):
    _subreddit = "cryptocurrency"
    meta = CollectorMeta(
        name="reddit_crypto",
        display_name="Reddit r/cryptocurrency Activity",
        update_frequency="hourly",
        api_docs_url="https://www.reddit.com/dev/api/",
        requires_key=True,
        domain="sentiment",
        category="attention",
    )


class RedditWsbCollector(_RedditSubredditCollector):
    _subreddit = "wallstreetbets"
    meta = CollectorMeta(
        name="reddit_wsb",
        display_name="Reddit r/wallstreetbets Activity",
        update_frequency="hourly",
        api_docs_url="https://www.reddit.com/dev/api/",
        requires_key=True,
        domain="sentiment",
        category="attention",
    )


class RedditBitcoinCollector(_RedditSubredditCollector):
    _subreddit = "Bitcoin"
    meta = CollectorMeta(
        name="reddit_bitcoin",
        display_name="Reddit r/Bitcoin Activity",
        update_frequency="hourly",
        api_docs_url="https://www.reddit.com/dev/api/",
        requires_key=True,
        domain="sentiment",
        category="attention",
    )
