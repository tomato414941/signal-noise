from __future__ import annotations

import os

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_BASE = "https://api.congress.gov/v3"
_CONGRESS_API_KEY: str | None = None


def _get_key() -> str:
    global _CONGRESS_API_KEY
    if _CONGRESS_API_KEY:
        return _CONGRESS_API_KEY
    key = os.environ.get("CONGRESS_API_KEY")
    if not key:
        secret_path = os.path.expanduser("~/.secrets/congress")
        if os.path.exists(secret_path):
            with open(secret_path) as f:
                for line in f:
                    if line.startswith("export CONGRESS_API_KEY="):
                        key = line.split("=", 1)[1].strip().strip("'\"")
                        break
    if not key:
        raise RuntimeError(
            "CONGRESS_API_KEY not set — get one at https://api.congress.gov/sign-up/"
        )
    _CONGRESS_API_KEY = key
    return key


# Congress numbers and their start years (each congress = 2 years)
# 110th Congress (2007-2008) through current
_CONGRESSES = list(range(110, 120))


class CongressBillCountCollector(BaseCollector):
    """Total bill count per Congress session (2-year period)."""

    meta = CollectorMeta(
        name="congress_bill_count",
        display_name="US Congress: Bills Introduced",
        update_frequency="yearly",
        api_docs_url="https://api.congress.gov/",
        requires_key=True,
        domain="society",
        category="legislation",
    )

    def fetch(self) -> pd.DataFrame:
        api_key = _get_key()
        rows = []
        for congress_num in _CONGRESSES:
            url = f"{_API_BASE}/bill/{congress_num}"
            resp = requests.get(
                url,
                params={"api_key": api_key, "limit": 1},
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            count = data.get("pagination", {}).get("count", 0)
            # Congress start year: 2 * congress_num + 1787
            start_year = 2 * congress_num + 1787
            rows.append({
                "date": pd.to_datetime(f"{start_year}-01-03", utc=True),
                "value": float(count),
            })

        if not rows:
            raise RuntimeError("No Congress.gov bill data")

        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


# Bill type breakdown per current congress
_BILL_TYPES = [
    ("hr", "congress_hr_count", "US Congress: House Bills (current)"),
    ("s", "congress_s_count", "US Congress: Senate Bills (current)"),
    ("hjres", "congress_hjres_count", "US Congress: House Joint Resolutions (current)"),
    ("sjres", "congress_sjres_count", "US Congress: Senate Joint Resolutions (current)"),
]


def _make_bill_type_collector(
    bill_type: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="yearly",
            api_docs_url="https://api.congress.gov/",
            requires_key=True,
            domain="society",
            category="legislation",
        )

        def fetch(self) -> pd.DataFrame:
            api_key = _get_key()
            rows = []
            for congress_num in _CONGRESSES:
                url = f"{_API_BASE}/bill/{congress_num}/{bill_type}"
                resp = requests.get(
                    url,
                    params={"api_key": api_key, "limit": 1},
                    timeout=self.config.request_timeout,
                )
                resp.raise_for_status()
                data = resp.json()
                count = data.get("pagination", {}).get("count", 0)
                start_year = 2 * congress_num + 1787
                rows.append({
                    "date": pd.to_datetime(f"{start_year}-01-03", utc=True),
                    "value": float(count),
                })

            if not rows:
                raise RuntimeError(f"No Congress.gov data for {bill_type}")

            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Congress_{name}"
    _Collector.__qualname__ = f"Congress_{name}"
    return _Collector


def get_congress_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {
        "congress_bill_count": CongressBillCountCollector,
    }
    for bill_type, name, display in _BILL_TYPES:
        collectors[name] = _make_bill_type_collector(bill_type, name, display)
    return collectors
