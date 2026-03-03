from __future__ import annotations


import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

# NSIDC Sea Ice Index v4 daily CSV files
_NSIDC_BASE = "https://noaadata.apps.nsidc.org/NOAA/G02135"

_SEA_ICE_SERIES = [
    ("north", "N", "nsidc_ice_north", "NSIDC Sea Ice Extent: Arctic"),
    ("south", "S", "nsidc_ice_south", "NSIDC Sea Ice Extent: Antarctic"),
]


def _make_nsidc_collector(
    hemisphere: str, prefix: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://nsidc.org/data/g02135/versions/4",
            domain="environment",
            category="cryosphere",
        )

        def fetch(self) -> pd.DataFrame:
            url = (
                f"{_NSIDC_BASE}/{hemisphere}/daily/data/"
                f"{prefix}_seaice_extent_daily_v4.0.csv"
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()

            text = resp.text
            # Skip header lines (comments start with spaces + "Year")
            lines = text.strip().split("\n")
            # Find the actual data start (skip comment/header rows)
            data_lines = []
            header_found = False
            for line in lines:
                stripped = line.strip()
                if not header_found:
                    if stripped.startswith("Year"):
                        header_found = True
                    continue
                if stripped and not stripped.startswith(","):
                    data_lines.append(stripped)

            if not data_lines:
                raise RuntimeError(f"No NSIDC data for {hemisphere}")

            rows = []
            for line in data_lines:
                parts = [p.strip() for p in line.split(",")]
                if len(parts) < 4:
                    continue
                try:
                    year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                    extent = float(parts[3])
                    if extent > 0:
                        dt = pd.Timestamp(year=year, month=month, day=day, tz="UTC")
                        rows.append({"date": dt, "value": extent})
                except (ValueError, TypeError):
                    continue

            if not rows:
                raise RuntimeError(f"No valid NSIDC extent data for {hemisphere}")

            df = pd.DataFrame(rows)
            # Return last 2 years only to keep manageable
            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=730)
            df = df[df["date"] >= cutoff]
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"NSIDC_{name}"
    _Collector.__qualname__ = f"NSIDC_{name}"
    return _Collector


def get_nsidc_collectors() -> dict[str, type[BaseCollector]]:
    return {t[2]: _make_nsidc_collector(*t) for t in _SEA_ICE_SERIES}
