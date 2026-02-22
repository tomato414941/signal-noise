from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class SantimentSocialVolumeCollector(BaseCollector):
    """Santiment BTC social volume (free tier)."""

    meta = CollectorMeta(
        name="santiment_social_btc",
        display_name="Santiment BTC Social Volume",
        update_frequency="daily",
        api_docs_url="https://api.santiment.net/",
        domain="sentiment",
        category="attention",
    )

    URL = "https://api.santiment.net/graphql"

    def fetch(self) -> pd.DataFrame:
        end = pd.Timestamp.now(tz="UTC")
        start = end - pd.Timedelta(days=90)
        query = {
            "query": """{
                getMetric(metric: "social_volume_total") {
                    timeseriesData(
                        slug: "bitcoin"
                        from: "%s"
                        to: "%s"
                        interval: "1d"
                    ) { datetime value }
                }
            }""" % (start.isoformat(), end.isoformat())
        }
        resp = requests.post(self.URL, json=query, timeout=self.config.request_timeout)
        resp.raise_for_status()
        ts_data = (
            resp.json()
            .get("data", {})
            .get("getMetric", {})
            .get("timeseriesData", [])
        )
        if not ts_data:
            raise RuntimeError("No Santiment data (may require API key)")
        rows = [
            {"date": pd.Timestamp(d["datetime"], tz="UTC"), "value": float(d["value"])}
            for d in ts_data
        ]
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
