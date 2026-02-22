from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class RIPEPeerCountCollector(BaseCollector):
    """RIPE RIS full-feed IPv4 peer count -- proxy for global BGP routing health.

    Queries the current snapshot of full-feed IPv4 peers visible
    to the RIPE Routing Information Service.
    """

    meta = CollectorMeta(
        name="ripe_peer_count",
        display_name="RIPE RIS IPv4 Full-Feed Peers",
        update_frequency="daily",
        api_docs_url="https://stat.ripe.net/docs/02.data-api/",
        domain="infrastructure",
        category="internet",
    )

    URL = "https://stat.ripe.net/data/ris-peers/data.json?query_time={ts}"

    def fetch(self) -> pd.DataFrame:
        now = pd.Timestamp.now(tz="UTC")
        url = self.URL.format(ts=now.strftime("%Y-%m-%dT08:00"))
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        peers = data.get("data", {}).get("peers", [])
        v4_full = sum(1 for p in peers if p.get("v4_full_table"))
        return pd.DataFrame(
            [{"date": now.normalize(), "value": float(v4_full)}]
        )
