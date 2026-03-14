"""Pub.dev (Dart/Flutter) package count collector.

Tracks total packages on the Dart/Flutter package registry.
Growth reflects Flutter ecosystem adoption velocity.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://pub.dev/api/package-name-completion-data"


class PubDevPackageCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="pubdev_packages",
        display_name="Pub.dev Total Packages (Dart/Flutter)",
        update_frequency="daily",
        api_docs_url="https://pub.dev/help/api",
        domain="technology",
        category="developer",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_API_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        packages = resp.json().get("packages", [])
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(len(packages))}])
