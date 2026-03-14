"""WordPress.org ecosystem stats.

Tracks plugin directory size, CMS version adoption, and PHP
version distribution across the WordPress ecosystem. WordPress
powers ~40% of the web, making these stats a proxy for web
technology migration velocity.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_PLUGIN_URL = "https://api.wordpress.org/plugins/info/1.2/"
_WP_VERSIONS_URL = "https://api.wordpress.org/stats/wordpress/1.0/"
_PHP_VERSIONS_URL = "https://api.wordpress.org/stats/php/1.0/"


class WPPluginCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="wp_plugin_count",
        display_name="WordPress Plugin Directory Count",
        update_frequency="daily",
        api_docs_url="https://codex.wordpress.org/WordPress.org_API",
        domain="technology",
        category="developer",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            _PLUGIN_URL,
            params={
                "action": "query_plugins",
                "request[page]": "1",
                "request[per_page]": "1",
                "request[browse]": "popular",
            },
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        total = resp.json().get("info", {}).get("results")
        if total is None:
            raise RuntimeError("No WordPress plugin count")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])


class WPLatestVersionShareCollector(BaseCollector):
    """Share of WordPress sites running the latest major version."""

    meta = CollectorMeta(
        name="wp_latest_version_share",
        display_name="WordPress Latest Version Share (%)",
        update_frequency="daily",
        api_docs_url="https://codex.wordpress.org/WordPress.org_API",
        domain="technology",
        category="developer",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_WP_VERSIONS_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        versions = resp.json()
        if not versions:
            raise RuntimeError("No WordPress version data")
        latest = max(versions.items(), key=lambda x: float(x[1]))
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(latest[1])}])


class WPModernPHPShareCollector(BaseCollector):
    """Share of WordPress sites running PHP 8.x+."""

    meta = CollectorMeta(
        name="wp_modern_php_share",
        display_name="WordPress PHP 8.x+ Share (%)",
        update_frequency="daily",
        api_docs_url="https://codex.wordpress.org/WordPress.org_API",
        domain="technology",
        category="developer",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_PHP_VERSIONS_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        versions = resp.json()
        if not versions:
            raise RuntimeError("No WordPress PHP version data")
        modern = sum(float(v) for ver, v in versions.items() if ver.startswith("8."))
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": round(modern, 3)}])
