from __future__ import annotations

import threading
import time

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_lock = threading.Lock()
_last_request: float = 0.0
_MIN_INTERVAL: float = 2.0
_HEADERS = {"User-Agent": "signal-noise/0.1 (https://github.com/user/signal-noise)"}


def _throttled_get(url: str, timeout: int = 30) -> dict:
    global _last_request
    with _lock:
        wait = _MIN_INTERVAL - (time.monotonic() - _last_request)
        if wait > 0:
            time.sleep(wait)
        resp = requests.get(url, headers=_HEADERS, timeout=timeout)
        _last_request = time.monotonic()
    resp.raise_for_status()
    return resp.json()


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
    ("wasm-bindgen", "crates_wasm_bindgen_downloads", "crates.io: wasm-bindgen"),
    ("tauri", "crates_tauri_downloads", "crates.io: tauri"),
    ("bevy", "crates_bevy_downloads", "crates.io: bevy"),
    ("solana-sdk", "crates_solana_sdk_downloads", "crates.io: solana-sdk"),
    ("actix-web", "crates_actix_web_downloads", "crates.io: actix-web"),
    ("diesel", "crates_diesel_downloads", "crates.io: diesel"),
    ("warp", "crates_warp_downloads", "crates.io: warp"),
    ("poem", "crates_poem_downloads", "crates.io: poem"),
    ("thiserror", "crates_thiserror_downloads", "crates.io: thiserror"),
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
            data = _throttled_get(url, timeout=self.config.request_timeout)
            extra = data.get("meta", {}).get("extra_downloads", [])
            if not extra:
                raise RuntimeError(f"No crates.io download data for {crate}")
            rows = [
                {
                    "date": pd.to_datetime(r["date"], utc=True),
                    "value": float(r["downloads"]),
                }
                for r in extra
                if r.get("downloads") is not None
            ]
            if not rows:
                raise RuntimeError(f"No crates.io download data for {crate}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Crates_{name}"
    _Collector.__qualname__ = f"Crates_{name}"
    return _Collector


def get_crates_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_crates_collector(*t) for t in CRATES}
