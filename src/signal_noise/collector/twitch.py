from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector._auth import load_secrets
from signal_noise.collector.base import BaseCollector, CollectorMeta

_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
_HELIX_URL = "https://api.twitch.tv/helix"

_TWITCH_TOKEN: str | None = None


def _get_credentials() -> tuple[str, str]:
    """Return (client_id, access_token). Obtain token via client credentials."""
    global _TWITCH_TOKEN
    creds = load_secrets("twitch", ["TWITCH_CLIENT_ID", "TWITCH_CLIENT_SECRET"],
                         signup_url="https://dev.twitch.tv/")
    client_id = creds["TWITCH_CLIENT_ID"]

    if _TWITCH_TOKEN:
        return client_id, _TWITCH_TOKEN

    resp = requests.post(_TOKEN_URL, params={
        "client_id": client_id,
        "client_secret": creds["TWITCH_CLIENT_SECRET"],
        "grant_type": "client_credentials",
    }, timeout=15)
    resp.raise_for_status()
    _TWITCH_TOKEN = resp.json()["access_token"]
    return client_id, _TWITCH_TOKEN


# Top games to track viewer counts for
_TWITCH_GAMES = [
    ("509658", "twitch_just_chatting", "Twitch Viewers: Just Chatting"),
    ("21779", "twitch_league", "Twitch Viewers: League of Legends"),
    ("33214", "twitch_fortnite", "Twitch Viewers: Fortnite"),
    ("32982", "twitch_gtav", "Twitch Viewers: GTA V"),
    ("516575", "twitch_valorant", "Twitch Viewers: Valorant"),
    ("32399", "twitch_csgo", "Twitch Viewers: Counter-Strike"),
]


def _make_twitch_collector(
    game_id: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="hourly",
            api_docs_url="https://dev.twitch.tv/docs/api/reference/#get-streams",
            requires_key=True,
            domain="sentiment",
            category="gaming",
        )

        def fetch(self) -> pd.DataFrame:
            client_id, token = _get_credentials()
            headers = {
                "Client-ID": client_id,
                "Authorization": f"Bearer {token}",
            }
            # Get top streams for this game
            resp = requests.get(
                f"{_HELIX_URL}/streams",
                params={"game_id": game_id, "first": "100"},
                headers=headers,
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            streams = resp.json().get("data", [])

            total_viewers = sum(s.get("viewer_count", 0) for s in streams)
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(total_viewers)}])

    _Collector.__name__ = f"Twitch_{name}"
    _Collector.__qualname__ = f"Twitch_{name}"
    return _Collector


class TwitchTotalViewersCollector(BaseCollector):
    """Total viewers across top 100 Twitch streams."""

    meta = CollectorMeta(
        name="twitch_total_viewers",
        display_name="Twitch: Total Top-100 Viewers",
        update_frequency="hourly",
        api_docs_url="https://dev.twitch.tv/docs/api/reference/#get-streams",
        requires_key=True,
        domain="sentiment",
        category="gaming",
    )

    def fetch(self) -> pd.DataFrame:
        client_id, token = _get_credentials()
        headers = {
            "Client-ID": client_id,
            "Authorization": f"Bearer {token}",
        }
        resp = requests.get(
            f"{_HELIX_URL}/streams",
            params={"first": "100"},
            headers=headers,
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        streams = resp.json().get("data", [])
        total = sum(s.get("viewer_count", 0) for s in streams)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])


def get_twitch_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {
        "twitch_total_viewers": TwitchTotalViewersCollector,
    }
    for game_id, name, display in _TWITCH_GAMES:
        collectors[name] = _make_twitch_collector(game_id, name, display)
    return collectors
