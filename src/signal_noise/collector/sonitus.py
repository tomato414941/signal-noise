from __future__ import annotations

import os
from datetime import datetime, timedelta, UTC
from pathlib import Path

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = "https://data.smartdublin.ie/sonitus-api/api"

_sonitus_auth: tuple[str, str] | None = None


def _get_sonitus_auth() -> tuple[str, str]:
    global _sonitus_auth
    if _sonitus_auth:
        return _sonitus_auth

    user = os.environ.get("SONITUS_USER")
    pw = os.environ.get("SONITUS_PASSWORD")

    if not user or not pw:
        secret_file = Path.home() / ".secrets" / "sonitus"
        if secret_file.exists():
            for line in secret_file.read_text().splitlines():
                if line.startswith("export SONITUS_USER="):
                    user = line.split("=", 1)[1].strip().strip("'\"")
                elif line.startswith("export SONITUS_PASSWORD="):
                    pw = line.split("=", 1)[1].strip().strip("'\"")

    if not user or not pw:
        raise RuntimeError(
            "SONITUS_USER / SONITUS_PASSWORD not set. "
            "Store in ~/.secrets/sonitus"
        )
    _sonitus_auth = (user, pw)
    return _sonitus_auth


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
            resp = requests.post(
                url, json=params,
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
