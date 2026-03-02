from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class SubmarineCableCollector(BaseCollector):
    """Cumulative count of submarine internet cables by ready-for-service year.

    Fetches the full cable list from TeleGeography's public API, then
    retrieves individual cable details to extract the RFS year.
    """

    meta = CollectorMeta(
        name="submarine_cable_count",
        display_name="Submarine Internet Cables (cumulative)",
        update_frequency="monthly",
        api_docs_url="https://www.submarinecablemap.com/",
        domain="technology",
        category="internet",
    )

    _LIST_URL = "https://www.submarinecablemap.com/api/v3/cable/all.json"
    _DETAIL_URL = "https://www.submarinecablemap.com/api/v3/cable/{cable_id}.json"

    _MAX_WORKERS = 20

    def _fetch_cable_rfs(self, cable_id: str) -> int | None:
        """Fetch a single cable's RFS year. Returns None on failure."""
        try:
            resp = requests.get(
                self._DETAIL_URL.format(cable_id=cable_id),
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            rfs = data.get("rfs_year") or data.get("rfs")
            if rfs is not None:
                return int(rfs)
        except (requests.RequestException, ValueError, TypeError):
            pass
        return None

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self._LIST_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        cables = resp.json()
        if not cables:
            raise RuntimeError("No submarine cable list data")

        cable_ids = [c["id"] for c in cables if "id" in c]

        # Fetch RFS years in parallel
        year_counts: dict[int, int] = {}
        with ThreadPoolExecutor(max_workers=self._MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._fetch_cable_rfs, cid): cid
                for cid in cable_ids
            }
            for future in as_completed(futures):
                rfs_year = future.result()
                if rfs_year is not None and 1850 < rfs_year < 2100:
                    year_counts[rfs_year] = year_counts.get(rfs_year, 0) + 1

        if not year_counts:
            raise RuntimeError("No submarine cable RFS data")

        rows = []
        cumulative = 0
        for year in sorted(year_counts):
            cumulative += year_counts[year]
            rows.append({
                "date": pd.Timestamp(f"{year}-01-01", tz="UTC"),
                "value": float(cumulative),
            })
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
