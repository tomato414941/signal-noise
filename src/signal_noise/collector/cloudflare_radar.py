from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CloudflareRadarCollector(BaseCollector):
    """Global HTTPS adoption percentage from Mozilla/Firefox telemetry.

    Uses the historical HTTPS adoption CSV published on Let's Encrypt's
    CDN (sourced from Firefox telemetry) showing the percentage of
    page-loads using TLS, as a proxy for internet security adoption.
    """

    meta = CollectorMeta(
        name="cloudflare_http_human",
        display_name="HTTPS Page-Load Adoption %",
        update_frequency="daily",
        api_docs_url="https://letsencrypt.org/stats/",
        domain="infrastructure",
        category="internet",
    )

    URL = "https://d4twhgtvn0ff5.cloudfront.net/historical-https-adoption.csv"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        raw = pd.read_csv(io.StringIO(resp.text))
        if raw.empty:
            raise RuntimeError("No HTTPS adoption data")
        raw["datestamp"] = pd.to_datetime(raw["datestamp"], utc=True)
        result = raw[["datestamp", "percentPageloadsAreTLS"]].copy()
        result.columns = ["timestamp", "value"]
        result["value"] = pd.to_numeric(result["value"], errors="coerce")
        result = result.dropna()
        if result.empty:
            raise RuntimeError("No parseable HTTPS adoption data")
        return result.sort_values("timestamp").reset_index(drop=True)


class LetsEncryptCertCountCollector(BaseCollector):
    """Let's Encrypt active certificate count.

    Proxy for HTTPS/TLS adoption growth and web infrastructure health.
    """

    meta = CollectorMeta(
        name="letsencrypt_cert_count",
        display_name="Let's Encrypt Active Certificates",
        update_frequency="daily",
        api_docs_url="https://letsencrypt.org/stats/",
        domain="infrastructure",
        category="internet",
    )

    URL = "https://d4twhgtvn0ff5.cloudfront.net/cert-timeline.csv"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        raw = pd.read_csv(io.StringIO(resp.text))
        if raw.empty:
            raise RuntimeError("No Let's Encrypt cert data")
        raw["datestamp"] = pd.to_datetime(raw.iloc[:, 0], utc=True)
        result = pd.DataFrame({
            "timestamp": raw["datestamp"],
            "value": pd.to_numeric(raw.iloc[:, 1], errors="coerce"),
        }).dropna()
        if result.empty:
            raise RuntimeError("No parseable cert data")
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365)
        result = result[result["timestamp"] >= cutoff]
        return result.sort_values("timestamp").reset_index(drop=True)
