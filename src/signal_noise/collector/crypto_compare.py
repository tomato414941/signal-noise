from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CryptoCompareVolumeCollector(BaseCollector):
    """CryptoCompare daily total exchange volume (BTC)."""

    meta = CollectorMeta(
        name="cryptocompare_volume",
        display_name="CryptoCompare Total Exchange Volume",
        update_frequency="daily",
        api_docs_url="https://min-api.cryptocompare.com/documentation",
        domain="markets",
        category="crypto",
    )

    URL = "https://min-api.cryptocompare.com/data/exchange/histoday?tsym=USD&limit=365"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("Data", [])
        if not data:
            raise RuntimeError("No CryptoCompare data")
        rows = [
            {
                "date": pd.Timestamp(d["time"], unit="s", tz="UTC"),
                "value": float(d.get("volume", 0)),
            }
            for d in data if "time" in d
        ]
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
