from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CosmicRayCollector(BaseCollector):
    """Neutron monitor cosmic ray flux from NMDB (Oulu station)."""

    meta = CollectorMeta(
        name="cosmic_ray_flux",
        display_name="Cosmic Ray Neutron Flux (Oulu)",
        update_frequency="hourly",
        api_docs_url="https://www.nmdb.eu/nest/help.php",
        domain="geophysical",
        category="space_weather",
    )

    URL = (
        "https://www.nmdb.eu/nest/draw_graph.php?formchk=1&stations[]=OULU"
        "&tabchoice=revori&dtype=corr_for_efficiency&tresession=60"
        "&date_choice=bydate&start_year={sy}&start_month={sm}&start_day=01"
        "&end_year={ey}&end_month={em}&end_day={ed}&output=ascii"
    )

    def fetch(self) -> pd.DataFrame:
        end = pd.Timestamp.now(tz="UTC")
        start = end - pd.Timedelta(days=90)
        url = self.URL.format(
            sy=start.year, sm=f"{start.month:02d}",
            ey=end.year, em=f"{end.month:02d}", ed=f"{end.day:02d}",
        )
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().split("\n"):
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("start"):
                continue
            parts = line.split(";")
            if len(parts) >= 2:
                try:
                    ts = pd.to_datetime(parts[0].strip(), utc=True)
                    val = float(parts[1].strip())
                    rows.append({"timestamp": ts, "value": val})
                except (ValueError, TypeError):
                    continue
        if not rows:
            raise RuntimeError("No cosmic ray data from NMDB")
        df = pd.DataFrame(rows)
        return df.sort_values("timestamp").reset_index(drop=True)
