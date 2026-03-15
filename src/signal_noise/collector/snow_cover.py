from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_RUTGERS_BASE = "https://climate.rutgers.edu/snowcover/files"

_SNOW_SERIES = [
    ("moncov.nhland.txt", "snow_cover_nh_land", "Snow Cover: NH Land (km²)"),
    ("moncov.eurasia.txt", "snow_cover_eurasia", "Snow Cover: Eurasia (km²)"),
    ("moncov.nam.txt", "snow_cover_namerica", "Snow Cover: North America (km²)"),
]


def _make_snow_collector(
    filename: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="monthly",
            api_docs_url="https://climate.rutgers.edu/snowcover/",
            domain="environment",
            category="cryosphere",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"{_RUTGERS_BASE}/{filename}"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()

            rows = []
            for line in resp.text.strip().split("\n"):
                parts = line.split()
                if len(parts) < 3:
                    continue
                try:
                    year, month = int(parts[0]), int(parts[1])
                    value = float(parts[2])
                    dt = pd.Timestamp(year=year, month=month, day=15, tz="UTC")
                    rows.append({"date": dt, "value": value})
                except (ValueError, TypeError):
                    continue

            if not rows:
                raise RuntimeError(f"No Rutgers snow cover data for {filename}")

            df = pd.DataFrame(rows)
            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1825)
            return df[df["date"] >= cutoff].sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Snow_{name}"
    _Collector.__qualname__ = f"Snow_{name}"
    return _Collector


def get_snow_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_snow_collector(*t) for t in _SNOW_SERIES}
