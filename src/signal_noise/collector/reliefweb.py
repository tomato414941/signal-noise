"""ReliefWeb humanitarian crisis report collector.

Docs: https://apidoc.rwlabs.org/
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector._auth import load_secret
from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://api.reliefweb.int/v2/reports"


def _get_appname() -> str:
    return load_secret("reliefweb", "RELIEFWEB_APPNAME", optional=True) or "signal-noise-research"


class ReliefWebCollector(BaseCollector):
    meta = CollectorMeta(
        name="reliefweb_disaster_count",
        display_name="ReliefWeb Disaster Report Count",
        update_frequency="daily",
        api_docs_url="https://apidoc.rwlabs.org/",
        domain="sentiment",
        category="attention",
    )

    def fetch(self) -> pd.DataFrame:
        appname = _get_appname()
        payload = {
            "appname": appname,
            "filter": {
                "field": "disaster",
                "value": {"exists": True},
            },
            "facets": [
                {
                    "field": "date.created",
                    "interval": "day",
                    "sort": "value:asc",
                }
            ],
            "limit": 0,
        }
        resp = requests.post(
            _API_URL, json=payload, timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json()

        facet_data = (
            data.get("embedded", {})
            .get("facets", {})
            .get("date.created", {})
            .get("data", [])
        )
        if not facet_data:
            raise RuntimeError("ReliefWeb returned no facet data")

        rows = [
            {
                "date": pd.to_datetime(item["value"], utc=True),
                "value": float(item["count"]),
            }
            for item in facet_data
            if "value" in item and "count" in item
        ]
        if not rows:
            raise RuntimeError("No valid ReliefWeb facet records")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
