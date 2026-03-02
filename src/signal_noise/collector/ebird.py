from __future__ import annotations

import os
import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


def _load_ebird_key() -> str:
    key = os.environ.get("EBIRD_API_KEY", "")
    if not key:
        secrets = os.path.expanduser("~/.secrets/ebird")
        if os.path.exists(secrets):
            for line in open(secrets):
                line = line.strip()
                if line.startswith("export EBIRD_API_KEY="):
                    key = line.split("=", 1)[1].strip("'\"")
    return key


class EBirdObservationsCollector(BaseCollector):
    """eBird — recent notable bird observations count (US)."""

    meta = CollectorMeta(
        name="ebird_observations_us",
        display_name="eBird US Notable Observations",
        update_frequency="daily",
        api_docs_url="https://documenter.getpostman.com/view/664302/S1ENwy59",
        requires_key=True,
        domain="environment",
        category="wildlife",
    )

    URL = "https://api.ebird.org/v2/data/obs/US/recent/notable"

    def fetch(self) -> pd.DataFrame:
        key = _load_ebird_key()
        if not key:
            raise RuntimeError(
                "eBird API key not found. Set EBIRD_API_KEY env var "
                "or create ~/.secrets/ebird"
            )
        resp = requests.get(
            self.URL,
            headers={"X-eBirdApiToken": key},
            params={"maxResults": 10000, "back": 14},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            raise RuntimeError("No eBird data returned")
        dates = pd.to_datetime(
            [obs["obsDt"] for obs in data if "obsDt" in obs], utc=True
        )
        daily = dates.value_counts().sort_index()
        df = pd.DataFrame({"date": daily.index, "value": daily.values})
        return df.sort_values("date").reset_index(drop=True)
