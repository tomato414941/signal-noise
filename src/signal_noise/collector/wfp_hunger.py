from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

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


_WFP_COUNTRIES = [
    ("AFG", "wfp_food_insecure_afg", "WFP Food Insecure: Afghanistan"),
    ("YEM", "wfp_food_insecure_yem", "WFP Food Insecure: Yemen"),
    ("SSD", "wfp_food_insecure_ssd", "WFP Food Insecure: South Sudan"),
    ("SDN", "wfp_food_insecure_sdn", "WFP Food Insecure: Sudan"),
    ("SOM", "wfp_food_insecure_som", "WFP Food Insecure: Somalia"),
    ("ETH", "wfp_food_insecure_eth", "WFP Food Insecure: Ethiopia"),
    ("NGA", "wfp_food_insecure_nga", "WFP Food Insecure: Nigeria"),
    ("COD", "wfp_food_insecure_cod", "WFP Food Insecure: DR Congo"),
    ("HTI", "wfp_food_insecure_hti", "WFP Food Insecure: Haiti"),
    ("SYR", "wfp_food_insecure_syr", "WFP Food Insecure: Syria"),
]


def _make_wfp_country_collector(
    iso3: str, name: str, display_name: str,
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

            for entry in countries:
                country = entry.get("country", {})
                if country.get("iso3") == iso3:
                    fcs = entry.get("metrics", {}).get("fcs", {})
                    people = fcs.get("people", 0)
                    now = pd.Timestamp.now(tz="UTC").normalize()
                    return pd.DataFrame([{"date": now, "value": float(people)}])

            raise RuntimeError(f"Country {iso3} not found in WFP data")

    _Collector.__name__ = f"WFP_{name}"
    _Collector.__qualname__ = f"WFP_{name}"
    return _Collector


def get_wfp_collectors() -> dict[str, type[BaseCollector]]:
    result: dict[str, type[BaseCollector]] = {}
    for iso3, name, display_name in _WFP_COUNTRIES:
        result[name] = _make_wfp_country_collector(iso3, name, display_name)
    return result
