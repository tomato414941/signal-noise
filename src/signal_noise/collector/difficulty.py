from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class DifficultyCollector(BaseCollector):
    """Bitcoin mining difficulty (adjusts ~every 2016 blocks / ~2 weeks).

    Rising difficulty = miners investing heavily = bullish conviction.
    Falling difficulty = miners capitulating.
    """

    meta = CollectorMeta(
        name="btc_difficulty",
        display_name="Bitcoin Mining Difficulty",
        update_frequency="daily",
        api_docs_url="https://mempool.space/docs/api",
        domain="financial",
        category="crypto",
    )

    URL = "https://mempool.space/api/v1/mining/difficulty-adjustments/2y"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        rows = []
        for entry in data:
            try:
                # Response format: [timestamp, height, difficulty, change]
                if isinstance(entry, list) and len(entry) >= 3:
                    ts = pd.to_datetime(entry[0], unit="s", utc=True)
                    diff = float(entry[2])
                else:
                    ts = pd.to_datetime(entry["timestamp"], unit="s", utc=True)
                    diff = float(entry["difficulty"])
                rows.append({"date": ts.normalize(), "value": diff})
            except (KeyError, ValueError, TypeError, IndexError):
                continue

        if not rows:
            raise RuntimeError("No difficulty data parsed")

        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
