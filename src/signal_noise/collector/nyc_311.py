from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

# (collector_name, display_name, optional complaint type)
NYC_311_SERIES: list[tuple[str, str, str | None]] = [
    ("nyc_311_complaints", "NYC 311 Daily Complaints", None),
    ("nyc_311_noise", "NYC 311 Noise Complaints", "Noise - Residential"),
    ("nyc_311_heat_hot_water", "NYC 311 Heat/Hot Water Complaints", "HEAT/HOT WATER"),
    ("nyc_311_illegal_parking", "NYC 311 Illegal Parking Complaints", "Illegal Parking"),
    ("nyc_311_street_light", "NYC 311 Street Light Condition Complaints", "Street Light Condition"),
]


def _make_nyc311_collector(
    name: str, display_name: str, complaint_type: str | None,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        """NYC Open Data 311 service request count."""

        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9",
            domain="society",
            category="city_stats",
        )

        def fetch(self) -> pd.DataFrame:
            since = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=90)).strftime("%Y-%m-%dT00:00:00")
            where = [f"created_date > '{since}'"]
            if complaint_type:
                safe = complaint_type.replace("'", "''")
                where.append(f"complaint_type = '{safe}'")
            params = {
                "$where": " AND ".join(where),
                "$select": "date_trunc_ymd(created_date) as day, count(*) as cnt",
                "$group": "date_trunc_ymd(created_date)",
                "$order": "day ASC",
                "$limit": 10000,
            }
            resp = requests.get(_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                raise RuntimeError(f"No NYC 311 data for {name}")
            rows = []
            for entry in data:
                try:
                    rows.append({
                        "date": pd.to_datetime(entry["day"], utc=True),
                        "value": int(entry["cnt"]),
                    })
                except (KeyError, ValueError, TypeError):
                    continue
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"NYC311_{name}"
    _Collector.__qualname__ = f"NYC311_{name}"
    return _Collector


NYC311ComplaintsCollector = _make_nyc311_collector(
    "nyc_311_complaints", "NYC 311 Daily Complaints", None,
)


def get_nyc311_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_nyc311_collector(name, display, complaint_type)
        for name, display, complaint_type in NYC_311_SERIES
    }

