from __future__ import annotations

import logging

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

log = logging.getLogger(__name__)

_WFP_HEADERS = {"User-Agent": "signal-noise/0.1 (research)"}


class WFPHungerGlobalCollector(BaseCollector):
    meta = CollectorMeta(
        name="wfp_food_insecure_global",
        display_name="WFP Food Insecure People (Global)",
        update_frequency="weekly",
        api_docs_url="https://hungermap.wfp.org/",
        domain="society",
        category="food_security",
    )

    URL = "https://api.hungermapdata.org/v1/foodsecurity/country"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, headers=_WFP_HEADERS, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        countries = data.get("body", {}).get("countries", [])
        if not countries:
            raise RuntimeError("No WFP HungerMap data")

        total_people = 0
        total_countries = 0
        for entry in countries:
            fcs = entry.get("metrics", {}).get("fcs", {})
            people = fcs.get("people")
            if people is not None:
                total_people += people
                total_countries += 1

        if total_countries == 0:
            raise RuntimeError("No WFP FCS data in response")

        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total_people)}])


# (iso3, wfp_id, signal_name, display_name)
_WFP_COUNTRIES = [
    ("AFG", 1, "wfp_food_insecure_afg", "WFP Food Insecure: Afghanistan"),
    ("YEM", 269, "wfp_food_insecure_yem", "WFP Food Insecure: Yemen"),
    ("SSD", 70001, "wfp_food_insecure_ssd", "WFP Food Insecure: South Sudan"),
    ("SDN", 40764, "wfp_food_insecure_sdn", "WFP Food Insecure: Sudan"),
    ("SOM", 226, "wfp_food_insecure_som", "WFP Food Insecure: Somalia"),
    ("ETH", 79, "wfp_food_insecure_eth", "WFP Food Insecure: Ethiopia"),
    ("NGA", 182, "wfp_food_insecure_nga", "WFP Food Insecure: Nigeria"),
    ("COD", 68, "wfp_food_insecure_cod", "WFP Food Insecure: DR Congo"),
    ("HTI", 108, "wfp_food_insecure_hti", "WFP Food Insecure: Haiti"),
    ("SYR", 238, "wfp_food_insecure_syr", "WFP Food Insecure: Syria"),
]


def _find_country(countries: list[dict], iso3: str, wfp_id: int) -> dict | None:
    """Find a country entry by iso3 or WFP numeric id."""
    for entry in countries:
        country = entry.get("country", {})
        if country.get("iso3") == iso3 or country.get("id") == wfp_id:
            return entry
    return None


def _make_wfp_country_collector(
    iso3: str, wfp_id: int, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="weekly",
            api_docs_url="https://hungermap.wfp.org/",
            domain="society",
            category="food_security",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(
                WFPHungerGlobalCollector.URL,
                headers=_WFP_HEADERS,
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            countries = data.get("body", {}).get("countries", [])

            entry = _find_country(countries, iso3, wfp_id)
            if entry is not None:
                fcs = entry.get("metrics", {}).get("fcs", {})
                people = fcs.get("people", 0)
                now = pd.Timestamp.now(tz="UTC").normalize()
                return pd.DataFrame([{"date": now, "value": float(people)}])

            # Country absent from bulk endpoint (conflict zones often lack
            # FCS survey data).  Record NaN so the signal stays alive and
            # will auto-recover when WFP restores coverage.
            log.warning(
                "%s (%s / id=%d) not in WFP bulk response — recording NaN",
                display_name, iso3, wfp_id,
            )
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float("nan")}])

    _Collector.__name__ = f"WFP_{name}"
    _Collector.__qualname__ = f"WFP_{name}"
    return _Collector


def get_wfp_collectors() -> dict[str, type[BaseCollector]]:
    result: dict[str, type[BaseCollector]] = {}
    for iso3, wfp_id, name, display_name in _WFP_COUNTRIES:
        result[name] = _make_wfp_country_collector(iso3, wfp_id, name, display_name)
    return result
