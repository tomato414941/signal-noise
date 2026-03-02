from __future__ import annotations

import os

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
_HELIX_URL = "https://api.twitch.tv/helix"

_TWITCH_CLIENT_ID: str | None = None
_TWITCH_TOKEN: str | None = None


def _get_credentials() -> tuple[str, str]:
    """Return (client_id, access_token). Obtain token via client credentials."""
    global _TWITCH_CLIENT_ID, _TWITCH_TOKEN
    if _TWITCH_CLIENT_ID and _TWITCH_TOKEN:
        return _TWITCH_CLIENT_ID, _TWITCH_TOKEN

    client_id = os.environ.get("TWITCH_CLIENT_ID")
    client_secret = os.environ.get("TWITCH_CLIENT_SECRET")

    if not client_id or not client_secret:
        secret_path = os.path.expanduser("~/.secrets/twitch")
        if os.path.exists(secret_path):
            with open(secret_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("export TWITCH_CLIENT_ID="):
                        client_id = line.split("=", 1)[1].strip().strip("'\"")
                    elif line.startswith("export TWITCH_CLIENT_SECRET="):
                        client_secret = line.split("=", 1)[1].strip().strip("'\"")

    if not client_id or not client_secret:
        raise RuntimeError(
            "TWITCH_CLIENT_ID/SECRET not set — register at https://dev.twitch.tv/"
        )

    # Client credentials flow
    resp = requests.post(_TOKEN_URL, params={
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }, timeout=15)
    resp.raise_for_status()
    token = resp.json()["access_token"]

    _TWITCH_CLIENT_ID = client_id
    _TWITCH_TOKEN = token
    return client_id, token


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
