from __future__ import annotations

import os
import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


def _load_iucn_token() -> str:
    token = os.environ.get("IUCN_API_TOKEN", "")
    if not token:
        secrets = os.path.expanduser("~/.secrets/iucn")
        if os.path.exists(secrets):
            for line in open(secrets):
                line = line.strip()
                if line.startswith("export IUCN_API_TOKEN="):
                    token = line.split("=", 1)[1].strip("'\"")
    return token


class IUCNThreatenedSpeciesCollector(BaseCollector):
    """IUCN Red List — total number of threatened species (from summary stats)."""

    meta = CollectorMeta(
        name="iucn_threatened_species",
        display_name="IUCN Threatened Species Count",
        update_frequency="yearly",
        api_docs_url="https://www.iucnredlist.org/resources/summary-statistics",
        requires_key=True,
        domain="animal",
        category="biodiversity",
    )

    def fetch(self) -> pd.DataFrame:
        token = _load_iucn_token()
        if not token:
            raise RuntimeError(
                "IUCN API token not found. Set IUCN_API_TOKEN env var "
                "or create ~/.secrets/iucn"
            )
        url = f"https://apiv3.iucnredlist.org/api/v3/speciescount?token={token}"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        count = int(data.get("count", 0))
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": count}])
