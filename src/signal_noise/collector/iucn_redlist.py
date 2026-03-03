from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector._auth import load_secret
from signal_noise.collector.base import BaseCollector, CollectorMeta


class IUCNThreatenedSpeciesCollector(BaseCollector):
    """IUCN Red List — total number of threatened species (from summary stats)."""

    meta = CollectorMeta(
        name="iucn_threatened_species",
        display_name="IUCN Threatened Species Count",
        update_frequency="yearly",
        api_docs_url="https://www.iucnredlist.org/resources/summary-statistics",
        requires_key=True,
        domain="environment",
        category="biodiversity",
    )

    def fetch(self) -> pd.DataFrame:
        token = load_secret("iucn", "IUCN_API_TOKEN",
                            signup_url="https://www.iucnredlist.org/")
        url = f"https://apiv3.iucnredlist.org/api/v3/speciescount?token={token}"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        count = int(data.get("count", 0))
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": count}])
