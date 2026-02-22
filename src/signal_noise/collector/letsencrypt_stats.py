from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class LetsEncryptCollector(BaseCollector):
    """Let's Encrypt daily certificate issuance count.

    Fetches aggregate issuance statistics from the
    Let's Encrypt stats endpoint.
    """

    meta = CollectorMeta(
        name="ssl_cert_issuance",
        display_name="Let's Encrypt Daily Certificates Issued",
        update_frequency="daily",
        api_docs_url="https://letsencrypt.org/stats/",
        domain="infrastructure",
        category="internet",
    )

    URL = "https://d4twhgtvn0ff5.cloudfront.net/stats/current"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        rows = []
        for entry in data.get("certificatesIssued", {}).get("items", []):
            try:
                rows.append({
                    "date": pd.Timestamp(entry["date"], tz="UTC"),
                    "value": float(entry["count"]),
                })
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No Let's Encrypt issuance data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
