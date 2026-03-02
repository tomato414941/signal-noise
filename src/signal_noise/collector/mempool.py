from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class MempoolSizeCollector(BaseCollector):
    """Bitcoin mempool size in vMB (virtual megabytes).

    Large mempool = network congestion = high demand for block space.
    mempool.space API is free, no key required.
    """

    meta = CollectorMeta(
        name="mempool_size",
        display_name="Bitcoin Mempool Size (vMB)",
        update_frequency="hourly",
        api_docs_url="https://mempool.space/docs/api",
        domain="markets",
        category="crypto",
    )

    URL = "https://mempool.space/api/v1/statistics/2y"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        rows = []
        for entry in data:
            try:
                ts = pd.to_datetime(entry["added"], unit="s", utc=True)
                # vsize in vbytes, convert to vMB
                vmb = entry["vbytes_per_second"] if "vbytes_per_second" in entry else entry.get("vsize", 0)
                rows.append({"timestamp": ts, "value": float(vmb)})
            except (KeyError, ValueError, TypeError):
                continue

        if not rows:
            raise RuntimeError("No mempool data parsed")

        df = pd.DataFrame(rows)
        return df.sort_values("timestamp").reset_index(drop=True)


class MempoolFeeCollector(BaseCollector):
    """Bitcoin recommended fee rate (sat/vB) for next-block confirmation.

    Spikes = urgency / FOMO. mempool.space provides current snapshot;
    data accumulates across runs via parquet append.
    """

    meta = CollectorMeta(
        name="mempool_fee",
        display_name="Bitcoin Fee Rate (sat/vB)",
        update_frequency="hourly",
        api_docs_url="https://mempool.space/docs/api",
        domain="markets",
        category="crypto",
    )

    URL = "https://mempool.space/api/v1/fees/recommended"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        fee = float(data.get("halfHourFee", data.get("fastestFee", 0)))

        return pd.DataFrame({"timestamp": [ts], "value": [fee]})
