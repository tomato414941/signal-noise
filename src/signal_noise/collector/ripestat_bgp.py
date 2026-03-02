from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_RIPESTAT_URL = "https://stat.ripe.net/data"


class BGPIPv4PrefixCountCollector(BaseCollector):
    """Global IPv4 BGP routing table prefix count from RIPE RIS."""

    meta = CollectorMeta(
        name="bgp_ipv4_prefix_count",
        display_name="BGP Global IPv4 Prefix Count",
        update_frequency="daily",
        api_docs_url="https://stat.ripe.net/docs/data-api/",
        domain="technology",
        category="internet",
    )

    def fetch(self) -> pd.DataFrame:
        url = f"{_RIPESTAT_URL}/routing-status/data.json"
        resp = requests.get(
            url,
            params={"resource": "0/0"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})

        visibility = data.get("visibility", {})
        v4 = visibility.get("v4", {})
        total_pfx = v4.get("total_space") or v4.get("ris_peers_seeing", 0)

        if not total_pfx:
            # Fallback: use first_seen stats
            first_seen = data.get("first_seen", {})
            total_pfx = first_seen.get("prefix", 0)

        if not total_pfx:
            raise RuntimeError("No BGP prefix count from RIPEstat")

        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total_pfx)}])


class BGPIPv6PrefixCountCollector(BaseCollector):
    """Global IPv6 BGP routing table prefix count from RIPE RIS."""

    meta = CollectorMeta(
        name="bgp_ipv6_prefix_count",
        display_name="BGP Global IPv6 Prefix Count",
        update_frequency="daily",
        api_docs_url="https://stat.ripe.net/docs/data-api/",
        domain="technology",
        category="internet",
    )

    def fetch(self) -> pd.DataFrame:
        url = f"{_RIPESTAT_URL}/routing-status/data.json"
        resp = requests.get(
            url,
            params={"resource": "::/0"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})

        visibility = data.get("visibility", {})
        v6 = visibility.get("v6", {})
        total_pfx = v6.get("total_space") or v6.get("ris_peers_seeing", 0)

        if not total_pfx:
            first_seen = data.get("first_seen", {})
            total_pfx = first_seen.get("prefix", 0)

        if not total_pfx:
            raise RuntimeError("No BGP IPv6 prefix count from RIPEstat")

        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total_pfx)}])
