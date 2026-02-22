from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CosmicRayCollector(BaseCollector):
    """NOAA GOES satellite high-energy proton flux as cosmic ray proxy.

    Uses the GOES primary satellite integral proton flux (>=100 MeV)
    as a proxy for galactic cosmic ray intensity.  Higher energy channels
    better represent the cosmic ray background.
    """

    meta = CollectorMeta(
        name="cosmic_ray_flux",
        display_name="Cosmic Ray Proton Flux (GOES >=100 MeV)",
        update_frequency="hourly",
        api_docs_url="https://www.swpc.noaa.gov/products/goes-proton-flux",
        domain="geophysical",
        category="space_weather",
    )

    URL = "https://services.swpc.noaa.gov/json/goes/primary/integral-protons-7-day.json"

    # Use the highest-energy channel as best cosmic ray proxy
    _ENERGY_CHANNEL = ">=100 MeV"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        rows = []
        for entry in data:
            if entry.get("energy") != self._ENERGY_CHANNEL:
                continue
            try:
                ts = pd.to_datetime(entry["time_tag"], utc=True)
                val = float(entry["flux"])
                rows.append({"timestamp": ts, "value": val})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No cosmic ray proxy data from GOES")
        df = pd.DataFrame(rows)
        return df.sort_values("timestamp").reset_index(drop=True)
