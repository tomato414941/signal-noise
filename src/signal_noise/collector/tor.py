from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class TorUsersCollector(BaseCollector):
    """Estimated daily Tor relay users worldwide.

    Rising anonymity usage may correlate with capital flight,
    censorship events, or crypto adoption in restricted regions.
    Tor Metrics provides free CSV downloads.
    """

    meta = SourceMeta(
        name="tor_users",
        display_name="Tor Daily Users (estimated)",
        update_frequency="daily",
        data_type="internet",
        api_docs_url="https://metrics.torproject.org/userstats-relay-country.html",
    )

    # CSV: date, country, users (we aggregate globally)
    URL = "https://metrics.torproject.org/userstats-relay-table.csv?start={start}&end={end}"

    def fetch(self) -> pd.DataFrame:
        end = pd.Timestamp.now(tz="UTC")
        start = end - pd.Timedelta(days=365 * 2)
        url = self.URL.format(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
        )
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()

        rows = []
        for line in resp.text.strip().split("\n"):
            if line.startswith("date") or line.startswith("#"):
                continue
            parts = line.split(",")
            if len(parts) < 3:
                continue
            try:
                date = pd.Timestamp(parts[0].strip(), tz="UTC")
                users = float(parts[2].strip())
                rows.append({"date": date, "value": users})
            except (ValueError, IndexError):
                continue

        if not rows:
            raise RuntimeError("No Tor user data parsed")

        df = pd.DataFrame(rows)
        # Aggregate by date (sum across countries)
        daily = df.groupby("date")["value"].sum().reset_index()
        return daily.sort_values("date").reset_index(drop=True)
