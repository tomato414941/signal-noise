from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class TorUsersCollector(BaseCollector):
    """Estimated daily Tor relay users worldwide.

    Rising anonymity usage may correlate with capital flight,
    censorship events, or crypto adoption in restricted regions.
    Tor Metrics provides free CSV downloads.
    """

    meta = CollectorMeta(
        name="tor_users",
        display_name="Tor Daily Users (estimated)",
        update_frequency="daily",
        api_docs_url="https://metrics.torproject.org/userstats-relay-country.html",
        domain="technology",
        category="internet",
    )

    # CSV: date, country, users, lower, upper, frac (country=all for global)
    URL = "https://metrics.torproject.org/userstats-relay-country.csv?start={start}&end={end}&country=all"

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
            if line.startswith("date") or line.startswith("#") or not line.strip():
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
        return df.sort_values("date").reset_index(drop=True)
