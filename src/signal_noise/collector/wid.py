from __future__ import annotations

import re
from urllib.parse import urljoin

import pandas as pd
import requests

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_PAGE_URL = "https://wid.world/data/"
_DEFAULT_APP_JS_URL = "https://wid.world/www-site/themes/default/js/app.js?v=3.08"
_WID_CONFIG_RE = re.compile(
    r"this\.apiKey\s*=\s*'(?P<api_key>[^']+)'.*?"
    r"this\.apiURL\s*=\s*'(?P<api_url>[^']+)'",
    re.DOTALL,
)
_APP_JS_RE = re.compile(
    r'<script[^>]+src=["\'](?P<src>[^"\']*themes/default/js/app\.js[^"\']*)["\']',
    re.IGNORECASE,
)
_wid_cache = SharedAPICache(ttl=3600)

# WID variable codes:
# sptinc_p99p100_z = pre-tax national income, top 1% share (%)
# sptinc_p90p100_z = pre-tax national income, top 10% share (%)
# sptinc_p0p50_z = pre-tax national income, bottom 50% share (%)
# shweal_p99p100_z = net personal wealth, top 1% share (%)

# (indicator, percentile, area_codes, name_prefix, display_prefix, domain, category)
_WID_SERIES: list[tuple[str, dict[str, str], str, str]] = [
    # Top 1% income share
    (
        "sptinc_p99p100_z",
        {"US": "us", "CN": "cn", "GB": "gb", "FR": "fr",
         "DE": "de", "JP": "jp", "IN": "in", "BR": "br"},
        "wid_top1_income",
        "WID Top 1% Income Share",
    ),
    # Bottom 50% income share
    (
        "sptinc_p0p50_z",
        {"US": "us", "CN": "cn", "GB": "gb", "FR": "fr",
         "DE": "de", "JP": "jp", "IN": "in", "BR": "br"},
        "wid_bottom50_income",
        "WID Bottom 50% Income Share",
    ),
    # Top 1% wealth share
    (
        "shweal_p99p100_z",
        {"US": "us", "GB": "gb", "FR": "fr", "DE": "de"},
        "wid_top1_wealth",
        "WID Top 1% Wealth Share",
    ),
]


def _get_wid_api_config(timeout: int = 30) -> tuple[str, str]:
    def _fetch() -> tuple[str, str]:
        headers = {"User-Agent": "signal-noise/0.1 (research)"}
        page = requests.get(_API_PAGE_URL, headers=headers, timeout=timeout)
        page.raise_for_status()

        app_js_url = _DEFAULT_APP_JS_URL
        match = _APP_JS_RE.search(page.text)
        if match is not None:
            app_js_url = urljoin(_API_PAGE_URL, match.group("src"))

        script = requests.get(app_js_url, headers=headers, timeout=timeout)
        script.raise_for_status()

        config_match = _WID_CONFIG_RE.search(script.text)
        if config_match is None:
            raise RuntimeError("Could not discover WID API configuration")

        api_url = config_match.group("api_url").rstrip("/") + "/"
        api_key = config_match.group("api_key")
        return api_url, api_key

    return _wid_cache.get_or_fetch("wid_api_config", _fetch, ttl=86400)


def _fetch_wid_indicator(indicator: str, timeout: int = 30) -> dict:
    def _fetch() -> dict:
        api_url, api_key = _get_wid_api_config(timeout)
        variable = indicator.removesuffix("_z")
        endpoint = "cousins-variables" if indicator.endswith("_z") else "countries-variables"
        params = {
            "countries": "all",
            "variables": variable,
            "currency": "eu",
            "exchange": "p",
            "base": "k",
            "base_year": "2015",
        }
        if endpoint == "cousins-variables":
            params["conversion"] = "none"
            params["decomposition"] = "false"

        resp = requests.get(
            urljoin(api_url, endpoint),
            params=params,
            headers={
                "User-Agent": "signal-noise/0.1 (research)",
                "x-api-key": api_key,
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected WID response for {indicator}")
        return data

    return _wid_cache.get_or_fetch(f"wid_indicator:{indicator}", _fetch)


def _extract_wid_rows(data: dict, area: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []

    for series in data.values():
        if not isinstance(series, list):
            continue
        for country_data in series:
            if not isinstance(country_data, dict) or area not in country_data:
                continue
            values = country_data[area].get("values")
            if not isinstance(values, list):
                continue
            for point in values:
                year = point.get("y")
                value = point.get("v")
                if year is None or value is None:
                    continue
                rows.append({
                    "date": pd.to_datetime(f"{int(year)}-01-01", utc=True),
                    "value": float(value),
                })
            if rows:
                return rows

    return rows


def _make_wid_collector(
    indicator: str, area: str, suffix: str,
    name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="yearly",
            api_docs_url="https://wid.world/data/",
            domain="economy",
            category="inequality",
        )

        def fetch(self) -> pd.DataFrame:
            data = _fetch_wid_indicator(indicator, timeout=self.config.request_timeout)
            rows = _extract_wid_rows(data, area)
            if not rows:
                raise RuntimeError(f"No WID data for {indicator}/{area}")

            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"WID_{name}"
    _Collector.__qualname__ = f"WID_{name}"
    return _Collector


def get_wid_collectors() -> dict[str, type[BaseCollector]]:
    collectors = {}
    for indicator, countries, name_prefix, display_prefix in _WID_SERIES:
        for area, suffix in countries.items():
            name = f"{name_prefix}_{suffix}"
            display = f"{display_prefix}: {area}"
            cls = _make_wid_collector(indicator, area, suffix, name, display)
            collectors[name] = cls
    return collectors
