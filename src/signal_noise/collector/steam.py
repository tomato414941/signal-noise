from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class SteamPlayersCollector(BaseCollector):
    """Steam current online player count (snapshot).

    Hypothesis: when people are gaming, they're not trading.
    Inversely, market panic might drive people away from gaming.
    Data accumulates across runs via SQLite store.
    """

    meta = CollectorMeta(
        name="steam_players",
        display_name="Steam Online Players",
        update_frequency="hourly",
        api_docs_url="https://store.steampowered.com/stats/",
        domain="sentiment",
        category="attention",
    )

    # Unofficial but stable endpoint returning current player counts
    URL = "https://store.steampowered.com/stats/userdata.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()

        # Response contains arrays of [timestamp_ms, player_count]
        # in multiple categories; we use the first (total online)
        rows = []
        for series in data:
            # Handle both old format (list of lists) and new format (dict with "data" key)
            points = series.get("data", series) if isinstance(series, dict) else series
            if not isinstance(points, list):
                continue
            for point in points:
                if not isinstance(point, list) or len(point) < 2:
                    continue
                try:
                    ts = pd.to_datetime(point[0], unit="ms", utc=True)
                    value = float(point[1])
                    rows.append({"timestamp": ts, "value": value})
                except (ValueError, TypeError):
                    continue
            if rows:
                break  # Use first series only

        if not rows:
            raise RuntimeError("No Steam player data parsed")

        df = pd.DataFrame(rows)
        return df.sort_values("timestamp").reset_index(drop=True)
