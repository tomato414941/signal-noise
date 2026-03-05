from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector._auth import load_secrets
from signal_noise.collector.base import BaseCollector, CollectorMeta

_URL = "https://api.acleddata.com/acled/read"

# (collector_name, display_name, optional event_type filter)
ACLED_SERIES: list[tuple[str, str, str | None]] = [
    ("acled_events_global", "ACLED Global Conflict Events (Weekly)", None),
    ("acled_battles_global", "ACLED Global Battles (Weekly)", "Battles"),
    ("acled_violence_civilians_global", "ACLED Global Violence Against Civilians (Weekly)", "Violence against civilians"),
    ("acled_explosions_global", "ACLED Global Explosions/Remote Violence (Weekly)", "Explosions/Remote violence"),
    ("acled_riots_global", "ACLED Global Riots (Weekly)", "Riots"),
]


def _load_rows(timeout: int, days: int = 365) -> list[dict]:
    creds = load_secrets("acled", ["ACLED_API_KEY", "ACLED_EMAIL"],
                         signup_url="https://apidocs.acleddata.com/")
    since = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)).strftime("%Y-%m-%d")
    params = {
        "key": creds["ACLED_API_KEY"],
        "email": creds["ACLED_EMAIL"],
        "event_date": f"{since}|",
        "event_date_where": "BETWEEN",
        "limit": 0,
        "fields": "event_date,event_type",
    }
    resp = requests.get(_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    if not data:
        raise RuntimeError("No ACLED data returned")
    return data


def _make_acled_collector(
    name: str, display_name: str, event_type: str | None,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        """ACLED global conflict events, weekly count."""

        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="weekly",
            api_docs_url="https://apidocs.acleddata.com/",
            requires_key=True,
            domain="society",
            category="armed_conflict",
        )

        def fetch(self) -> pd.DataFrame:
            data = _load_rows(timeout=60)
            rows = data
            if event_type:
                rows = [e for e in data if str(e.get("event_type", "")).strip() == event_type]
            if not rows:
                raise RuntimeError(f"No ACLED rows for event_type={event_type}")
            dates = pd.to_datetime([e["event_date"] for e in rows], utc=True, errors="coerce")
            dates = dates.dropna()
            if dates.empty:
                raise RuntimeError("No parseable ACLED event dates")
            daily = dates.value_counts().sort_index()
            weekly = daily.resample("W").sum()
            df = pd.DataFrame({"date": weekly.index, "value": weekly.values})
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"ACLED_{name}"
    _Collector.__qualname__ = f"ACLED_{name}"
    return _Collector


ACLEDEventsCollector = _make_acled_collector(
    "acled_events_global", "ACLED Global Conflict Events (Weekly)", None,
)


def get_acled_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_acled_collector(name, display, event_type)
        for name, display, event_type in ACLED_SERIES
    }

