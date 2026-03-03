from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector._auth import load_secrets
from signal_noise.collector.base import BaseCollector, CollectorMeta


class ACLEDEventsCollector(BaseCollector):
    """ACLED global conflict events — weekly count of political violence events."""

    meta = CollectorMeta(
        name="acled_events_global",
        display_name="ACLED Global Conflict Events (Weekly)",
        update_frequency="weekly",
        api_docs_url="https://apidocs.acleddata.com/",
        requires_key=True,
        domain="society",
        category="armed_conflict",
    )

    URL = "https://api.acleddata.com/acled/read"

    def fetch(self) -> pd.DataFrame:
        creds = load_secrets("acled", ["ACLED_API_KEY", "ACLED_EMAIL"],
                             signup_url="https://apidocs.acleddata.com/")
        since = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365)).strftime("%Y-%m-%d")
        params = {
            "key": creds["ACLED_API_KEY"],
            "email": creds["ACLED_EMAIL"],
            "event_date": f"{since}|",
            "event_date_where": "BETWEEN",
            "limit": 0,
            "fields": "event_date",
        }
        resp = requests.get(self.URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            raise RuntimeError("No ACLED data returned")
        dates = pd.to_datetime([e["event_date"] for e in data], utc=True)
        daily = dates.value_counts().sort_index()
        weekly = daily.resample("W").sum()
        df = pd.DataFrame({"date": weekly.index, "value": weekly.values})
        return df.sort_values("date").reset_index(drop=True)
