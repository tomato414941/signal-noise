from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class HomebrewInstallsCollector(BaseCollector):
    """Homebrew install events for wget (proxy for macOS developer activity).

    Fetches formula analytics from the Homebrew Formulae API
    and records the 30-day install count as a daily snapshot.
    """

    meta = CollectorMeta(
        name="homebrew_wget_installs",
        display_name="Homebrew wget Install Events (30d)",
        update_frequency="daily",
        api_docs_url="https://formulae.brew.sh/analytics/",
        domain="technology",
        category="developer",
    )

    URL = "https://formulae.brew.sh/api/formula/wget.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        installs_30d = data.get("analytics", {}).get("install", {}).get("30d", {})
        total = 0
        if isinstance(installs_30d, dict):
            # The structure is {"wget": count} under 30d
            for key, val in installs_30d.items():
                if isinstance(val, (int, float)):
                    total += val
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])


class HomebrewFormulaeCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="brew_formulae_count",
        display_name="Homebrew Formulae (Total)",
        update_frequency="daily",
        api_docs_url="https://formulae.brew.sh/docs/api/",
        domain="technology",
        category="developer",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            "https://formulae.brew.sh/api/formula.json",
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        count = len(resp.json())
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])


class HomebrewCasksCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="brew_casks_count",
        display_name="Homebrew Casks (Total)",
        update_frequency="daily",
        api_docs_url="https://formulae.brew.sh/docs/api/",
        domain="technology",
        category="developer",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            "https://formulae.brew.sh/api/cask.json",
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        count = len(resp.json())
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])
