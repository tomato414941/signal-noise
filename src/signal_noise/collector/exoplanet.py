"""NASA Exoplanet Archive collector.

Tracks total confirmed exoplanets. Growth reflects the pace of
planetary discovery across missions like TESS, Kepler, and JWST.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_TAP_URL = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"


class ExoplanetCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="nasa_exoplanet_count",
        display_name="NASA Confirmed Exoplanets",
        update_frequency="weekly",
        api_docs_url="https://exoplanetarchive.ipac.caltech.edu/docs/TAP/usingTAP.html",
        domain="environment",
        category="celestial",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            _TAP_URL,
            params={"query": "SELECT COUNT(*) FROM ps", "format": "json"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            raise RuntimeError("No exoplanet count")
        count = data[0].get("count(*)")
        if count is None:
            raise RuntimeError("No exoplanet count in response")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])
