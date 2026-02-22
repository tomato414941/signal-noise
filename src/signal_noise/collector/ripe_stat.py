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

    # Threshold for "full table": a peer carrying >= this many v4 prefixes
    _V4_FULL_TABLE_THRESHOLD = 900_000

    def fetch(self) -> pd.DataFrame:
        now = pd.Timestamp.now(tz="UTC")
        url = self.URL.format(ts=now.strftime("%Y-%m-%dT08:00"))
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        peers_by_rrc = data.get("data", {}).get("peers", {})
        # peers_by_rrc is a dict: {"rrc00": [{asn, ip, v4_prefix_count, ...}], ...}
        v4_full = 0
        if isinstance(peers_by_rrc, dict):
            for rrc_peers in peers_by_rrc.values():
                for p in rrc_peers:
                    if p.get("v4_prefix_count", 0) >= self._V4_FULL_TABLE_THRESHOLD:
                        v4_full += 1
        elif isinstance(peers_by_rrc, list):
            # Fallback if API ever returns a flat list
            v4_full = sum(
                1 for p in peers_by_rrc
                if p.get("v4_prefix_count", 0) >= self._V4_FULL_TABLE_THRESHOLD
            )
        return pd.DataFrame(
            [{"date": now.normalize(), "value": float(v4_full)}]
        )
