from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class LetsEncryptCollector(BaseCollector):
    """Let's Encrypt daily certificate issuance count.

    Fetches the cert-timeline TSV published on Let's Encrypt's CDN.
    Columns: date, issued, active_certs, active_fqdns, active_registered_domains.
    """

    meta = CollectorMeta(
        name="ssl_cert_issuance",
        display_name="Let's Encrypt Daily Certificates Issued",
        update_frequency="daily",
        api_docs_url="https://letsencrypt.org/stats/",
        domain="infrastructure",
        category="internet",
    )

    URL = "https://d4twhgtvn0ff5.cloudfront.net/cert-timeline.tsv"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        df = pd.read_csv(
            io.StringIO(resp.text),
            sep="\t",
            header=None,
            names=["date", "issued", "active_certs", "active_fqdns", "active_regdoms"],
        )
        if df.empty:
            raise RuntimeError("No Let's Encrypt issuance data")
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df["value"] = pd.to_numeric(df["issued"], errors="coerce")
        result = df[["date", "value"]].dropna().copy()
        return result.sort_values("date").reset_index(drop=True)
