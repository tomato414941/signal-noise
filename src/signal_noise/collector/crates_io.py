from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (crate_name, collector_name, display_name)
CRATES: list[tuple[str, str, str]] = [
    ("serde", "crates_serde_downloads", "crates.io: serde"),
    ("tokio", "crates_tokio_downloads", "crates.io: tokio"),
    ("reqwest", "crates_reqwest_downloads", "crates.io: reqwest"),
    ("clap", "crates_clap_downloads", "crates.io: clap"),
    ("rand", "crates_rand_downloads", "crates.io: rand"),
    ("serde_json", "crates_serde_json_downloads", "crates.io: serde_json"),
    ("axum", "crates_axum_downloads", "crates.io: axum"),
    ("sqlx", "crates_sqlx_downloads", "crates.io: sqlx"),
    ("tracing", "crates_tracing_downloads", "crates.io: tracing"),
    ("anyhow", "crates_anyhow_downloads", "crates.io: anyhow"),
]


def _make_crates_collector(
    crate: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://crates.io/policies",
            domain="technology",
            category="developer",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://crates.io/api/v1/crates/{crate}/downloads"
            headers = {"User-Agent": "signal-noise/0.1 (research project)"}
            resp = requests.get(
                url, headers=headers, timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            entries = resp.json().get("version_downloads", [])
            rows = []
            for entry in entries:
                try:
                    rows.append({
                        "date": pd.Timestamp(entry["date"], tz="UTC"),
                        "value": float(entry["downloads"]),
                    })
                except (KeyError, ValueError, TypeError):
                    continue
            if not rows:
                raise RuntimeError(f"No crates.io download data for {crate}")
            df = pd.DataFrame(rows)
            daily = df.groupby("date")["value"].sum().reset_index()
            return daily.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Crates_{name}"
    _Collector.__qualname__ = f"Crates_{name}"
    return _Collector


def get_crates_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_crates_collector(*t) for t in CRATES}
