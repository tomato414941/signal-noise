"""NIST National Vulnerability Database (NVD) CVE stats.

Tracks total published CVEs. Growth rate reflects vulnerability
disclosure velocity across the software ecosystem.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0"


class NVDCVETotalCollector(BaseCollector):
    meta = CollectorMeta(
        name="nvd_cve_total",
        display_name="NVD Total Published CVEs",
        update_frequency="daily",
        api_docs_url="https://nvd.nist.gov/developers/vulnerabilities",
        domain="technology",
        category="internet",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            _API_URL,
            params={"resultsPerPage": "1"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        total = resp.json().get("totalResults")
        if total is None:
            raise RuntimeError("No NVD CVE total count")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])
