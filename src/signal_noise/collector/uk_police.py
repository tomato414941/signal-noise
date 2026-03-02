from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (category_slug, collector_name, display_name)
UK_CRIME_CATEGORIES: list[tuple[str, str, str]] = [
    ("violent-crime", "uk_violent_crime", "UK Crime: Violence"),
    ("burglary", "uk_burglary", "UK Crime: Burglary"),
    ("anti-social-behaviour", "uk_asb", "UK Crime: Anti-Social Behaviour"),
    ("robbery", "uk_robbery", "UK Crime: Robbery"),
    ("vehicle-crime", "uk_vehicle_crime", "UK Crime: Vehicle Crime"),
    ("shoplifting", "uk_shoplifting", "UK Crime: Shoplifting"),
    ("criminal-damage-arson", "uk_criminal_damage", "UK Crime: Criminal Damage"),
    ("drugs", "uk_drugs", "UK Crime: Drugs"),
    ("theft-from-the-person", "uk_theft_person", "UK Crime: Theft from Person"),
    ("possession-of-weapons", "uk_weapons", "UK Crime: Weapon Possession"),
]


def _make_uk_crime_collector(
    category_slug: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="monthly",
            api_docs_url="https://data.police.uk/docs/",
            domain="society",
            category="crime",
        )

        def fetch(self) -> pd.DataFrame:
            # Fetch all available date ranges
            dates_resp = requests.get(
                "https://data.police.uk/api/crimes-street-dates",
                timeout=self.config.request_timeout,
            )
            dates_resp.raise_for_status()
            available = dates_resp.json()
            if not available:
                raise RuntimeError("No UK crime dates available")

            rows = []
            # Get last 24 months of data (England & Wales level)
            for entry in available[:24]:
                date_str = entry["date"]  # "YYYY-MM"
                url = (
                    f"https://data.police.uk/api/crimes-no-location"
                    f"?category={category_slug}&force=metropolitan&date={date_str}"
                )
                resp = requests.get(url, timeout=self.config.request_timeout)
                if resp.status_code == 200:
                    crimes = resp.json()
                    count = len(crimes)
                    dt = pd.Timestamp(f"{date_str}-01", tz="UTC")
                    rows.append({"date": dt, "value": float(count)})

            if not rows:
                raise RuntimeError(f"No UK crime data for {category_slug}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"UKCrime{name.title()}Collector"
    _Collector.__qualname__ = _Collector.__name__
    return _Collector


def get_uk_crime_collectors() -> dict[str, type[BaseCollector]]:
    result = {}
    for slug, name, display in UK_CRIME_CATEGORIES:
        cls = _make_uk_crime_collector(slug, name, display)
        result[name] = cls
    return result
