from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class FreightosBDICollector(BaseCollector):
    """Freightos Baltic Daily Index — global container freight rate proxy."""

    meta = CollectorMeta(
        name="freightos_bdi",
        display_name="Freightos Baltic Daily Index",
        update_frequency="daily",
        api_docs_url="https://fbx.freightos.com/",
        domain="infrastructure",
        category="logistics",
    )

    URL = "https://fbx.freightos.com/api/lane/FBX?format=json"

    def fetch(self) -> pd.DataFrame:
        headers = {"Accept": "application/json"}
        resp = requests.get(self.URL, headers=headers, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        items = data if isinstance(data, list) else data.get("data", data.get("items", []))
        if not items:
            raise RuntimeError("No Freightos BDI data")
        rows = []
        for item in items:
            try:
                dt = pd.Timestamp(item.get("date") or item.get("timestamp"), tz="UTC")
                val = float(item.get("value") or item.get("price", 0))
                rows.append({"date": dt.normalize(), "value": val})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No parseable Freightos data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
