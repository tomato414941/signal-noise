from __future__ import annotations

import os
import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


def _load_acled_key() -> tuple[str, str]:
    key = os.environ.get("ACLED_API_KEY", "")
    email = os.environ.get("ACLED_EMAIL", "")
    if not key or not email:
        secrets = os.path.expanduser("~/.secrets/acled")
        if os.path.exists(secrets):
            for line in open(secrets):
                line = line.strip()
                if line.startswith("export ACLED_API_KEY="):
                    key = line.split("=", 1)[1].strip("'\"")
                elif line.startswith("export ACLED_EMAIL="):
                    email = line.split("=", 1)[1].strip("'\"")
    return key, email


class ACLEDEventsCollector(BaseCollector):
    """ACLED global conflict events — weekly count of political violence events."""

    meta = CollectorMeta(
        name="acled_events_global",
        display_name="ACLED Global Conflict Events (Weekly)",
        update_frequency="weekly",
        api_docs_url="https://apidocs.acleddata.com/",
        requires_key=True,
        domain="conflict",
        category="armed_conflict",
    )

    URL = "https://api.acleddata.com/acled/read"

    def fetch(self) -> pd.DataFrame:
        key, email = _load_acled_key()
        if not key or not email:
            raise RuntimeError(
                "ACLED credentials not found. Set ACLED_API_KEY and ACLED_EMAIL "
                "env vars or create ~/.secrets/acled"
            )
        since = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365)).strftime("%Y-%m-%d")
        params = {
            "key": key,
            "email": email,
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
