from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class GVPVolcanoCollector(BaseCollector):
    """Smithsonian GVP Holocene eruptions via OGC Web Feature Service.

    Queries the GeoServer WFS for confirmed Holocene eruptions and
    counts eruptions starting per decade to build a long-term timeline.
    """

    meta = CollectorMeta(
        name="gvp_active_volcanoes",
        display_name="GVP Holocene Eruptions (per decade)",
        update_frequency="weekly",
        api_docs_url="https://volcano.si.edu/database/webservices.cfm",
        domain="environment",
        category="seismic",
    )

    URL = (
        "https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows"
        "?service=WFS&version=1.0.0&request=GetFeature"
        "&typeName=GVP-VOTW:Smithsonian_VOTW_Holocene_Eruptions"
        "&outputFormat=application/json"
        "&maxFeatures=10000"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        if not features:
            raise RuntimeError("No GVP eruption features from WFS")
        rows = []
        for feat in features:
            props = feat.get("properties", {})
            year = props.get("StartDateYear")
            if year is None:
                continue
            try:
                year = int(year)
            except (ValueError, TypeError):
                continue
            # Skip far-past eruptions to keep data manageable
            if year < 1800:
                continue
            month = props.get("StartDateMonth") or 1
            day = props.get("StartDateDay") or 1
            if month == 0:
                month = 1
            if day == 0:
                day = 1
            try:
                ts = pd.Timestamp(year=year, month=int(month), day=int(day), tz="UTC")
                rows.append({"date": ts.normalize()})
            except (ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No GVP volcano data after parsing")
        df = pd.DataFrame(rows)
        # Count eruptions starting per month
        df["month"] = df["date"].dt.tz_localize(None).dt.to_period("M").dt.to_timestamp()
        monthly = df.groupby("month").size().reset_index(name="value")
        monthly.columns = ["date", "value"]
        monthly["date"] = pd.to_datetime(monthly["date"], utc=True)
        monthly["value"] = monthly["value"].astype(float)
        return monthly.sort_values("date").reset_index(drop=True)
