from __future__ import annotations

from datetime import datetime, timedelta, UTC

import pandas as pd
import requests

from signal_noise.collector._auth import load_secrets
from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = "https://data.smartdublin.ie/sonitus-api/api"


def _get_sonitus_auth() -> tuple[str, str]:
    creds = load_secrets("sonitus", ["SONITUS_USER", "SONITUS_PASSWORD"],
                         signup_url="https://data.smartdublin.ie/")
    return creds["SONITUS_USER"], creds["SONITUS_PASSWORD"]


# Dublin City Council noise monitors with known serial numbers
_MONITORS = [
    ("DCC-01", "sonitus_ballyfermot", "Dublin Noise: Ballyfermot"),
    ("DCC-02", "sonitus_walkinstown", "Dublin Noise: Walkinstown"),
    ("DCC-04", "sonitus_navan_rd", "Dublin Noise: Navan Road"),
    ("DCC-05", "sonitus_woodstock", "Dublin Noise: Woodstock Gardens"),
]


def _make_sonitus_collector(
    serial: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="hourly",
            api_docs_url="https://data.smartdublin.ie/sonitus-api",
            requires_key=True,
            domain="environment",
            category="noise",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=30)
            url = f"{_BASE_URL}/hourly-averages"
            params = {
                "monitor": serial,
                "start": start.strftime("%Y-%m-%dT%H:%M:%S"),
                "end": end.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            resp = requests.get(
                url, params=params,
                auth=_get_sonitus_auth(),
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            if not data:
                raise RuntimeError(f"No Sonitus data for {serial}")

            rows = []
            for entry in data:
                dt = entry.get("datetime") or entry.get("date")
                laeq = entry.get("laeq")
                if dt and laeq is not None:
                    rows.append({
                        "date": pd.to_datetime(dt, utc=True),
                        "value": float(laeq),
                    })

            if not rows:
                raise RuntimeError(f"No Sonitus laeq data for {serial}")

            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Sonitus_{name}"
    _Collector.__qualname__ = f"Sonitus_{name}"
    return _Collector


def get_sonitus_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_sonitus_collector(*t) for t in _MONITORS}
