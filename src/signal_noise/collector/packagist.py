"""Packagist (PHP/Composer) ecosystem stats.

Tracks total downloads, package count, and per-package downloads
for major PHP frameworks. Reflects PHP ecosystem health.
"""
from __future__ import annotations

import time

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_STATS_URL = "https://packagist.org/statistics.json"
_PKG_URL = "https://packagist.org/packages/{pkg}.json"

_stats_cache: dict | None = None
_stats_cache_ts: float = 0.0


def _fetch_stats(timeout: int = 30) -> dict:
    global _stats_cache, _stats_cache_ts
    now = time.monotonic()
    if _stats_cache is not None and (now - _stats_cache_ts) < 600:
        return _stats_cache
    resp = requests.get(_STATS_URL, timeout=timeout)
    resp.raise_for_status()
    _stats_cache = resp.json()
    _stats_cache_ts = now
    return _stats_cache


class PackagistTotalDownloadsCollector(BaseCollector):
    meta = CollectorMeta(
        name="packagist_total_downloads",
        display_name="Packagist Total Downloads",
        update_frequency="daily",
        api_docs_url="https://packagist.org/apidoc",
        domain="technology",
        category="developer",
    )

    def fetch(self) -> pd.DataFrame:
        data = _fetch_stats(timeout=self.config.request_timeout)
        val = data.get("totals", {}).get("downloads")
        if val is None:
            raise RuntimeError("No Packagist download count")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(val)}])


class PackagistPackageCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="packagist_packages",
        display_name="Packagist Total Packages",
        update_frequency="daily",
        api_docs_url="https://packagist.org/apidoc",
        domain="technology",
        category="developer",
    )

    def fetch(self) -> pd.DataFrame:
        data = _fetch_stats(timeout=self.config.request_timeout)
        val = data.get("totals", {}).get("packages")
        if val is None:
            raise RuntimeError("No Packagist package count")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(val)}])


def _make_packagist_pkg_collector(
    name: str, display_name: str, pkg: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://packagist.org/apidoc",
            domain="technology",
            category="developer",
        )

        def fetch(self) -> pd.DataFrame:
            url = _PKG_URL.format(pkg=pkg)
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            downloads = resp.json().get("package", {}).get("downloads", {}).get("total")
            if downloads is None:
                raise RuntimeError(f"No Packagist data for {pkg}")
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(downloads)}])

    _Collector.__name__ = f"Packagist_{name}"
    _Collector.__qualname__ = f"Packagist_{name}"
    return _Collector


_PKG_SIGNALS: list[tuple[str, str, str]] = [
    ("packagist_laravel", "Packagist laravel/framework Downloads", "laravel/framework"),
    ("packagist_symfony", "Packagist symfony/symfony Downloads", "symfony/symfony"),
    ("packagist_phpunit", "Packagist phpunit/phpunit Downloads", "phpunit/phpunit"),
    ("packagist_guzzle", "Packagist guzzlehttp/guzzle Downloads", "guzzlehttp/guzzle"),
    ("packagist_monolog", "Packagist monolog/monolog Downloads", "monolog/monolog"),
]


def get_packagist_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for name, display, pkg in _PKG_SIGNALS:
        collectors[name] = _make_packagist_pkg_collector(name, display, pkg)
    return collectors
