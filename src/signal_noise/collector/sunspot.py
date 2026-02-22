from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class SunspotCollector(BaseCollector):
    """Daily international sunspot number from SILSO (Royal Observatory of Belgium).

    Full historical record since 1818; we keep last 3 years.
    11-year solar cycle is a well-studied phenomenon.
    """

    meta = SourceMeta(
        name="sunspot",
        display_name="Daily Sunspot Number (SILSO)",
        update_frequency="daily",
        data_type="space_weather",
        api_docs_url="https://www.sidc.be/SILSO/datafiles",
        domain="geophysical",
        category="space_weather",
    )

    URL = "https://www.sidc.be/SILSO/INFO/sndtotcsv.php"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()

        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 3)
        rows = []
        for line in resp.text.strip().split("\n"):
            parts = [p.strip() for p in line.split(";")]
            if len(parts) < 5:
                continue
            try:
                year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                sn = float(parts[4])
                if sn < 0:
                    continue
                date = pd.Timestamp(year=year, month=month, day=day, tz="UTC")
                if date < cutoff:
                    continue
                rows.append({"date": date, "value": sn})
            except (ValueError, IndexError):
                continue

        if not rows:
            raise RuntimeError("No sunspot data parsed from SILSO")

        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
