from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (app_id, collector_name, display_name)
STEAM_GAMES: list[tuple[str, str, str]] = [
    ("730", "steam_cs2", "Steam Players: CS2"),
    ("570", "steam_dota2", "Steam Players: Dota 2"),
    ("578080", "steam_pubg", "Steam Players: PUBG"),
    ("1172470", "steam_apex", "Steam Players: Apex Legends"),
    ("440", "steam_tf2", "Steam Players: Team Fortress 2"),
    ("252490", "steam_rust", "Steam Players: Rust"),
    ("271590", "steam_gta5", "Steam Players: GTA V"),
    ("1245620", "steam_elden_ring", "Steam Players: Elden Ring"),
    ("1086940", "steam_baldurs_gate3", "Steam Players: Baldur's Gate 3"),
    ("359550", "steam_rainbow6", "Steam Players: Rainbow Six Siege"),
]


def _make_steam_collector(
    app_id: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="hourly",
            api_docs_url=f"https://steamcharts.com/app/{app_id}",
            domain="sentiment",
            category="gaming",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://steamcharts.com/app/{app_id}/chart-data.json"
            resp = requests.get(
                url,
                timeout=self.config.request_timeout,
                headers={"User-Agent": "signal-noise/1.0"},
            )
            resp.raise_for_status()
            data = resp.json()
            if not data:
                raise RuntimeError(f"No Steam data for {app_id}")
            rows = []
            for ts_ms, players in data:
                dt = pd.Timestamp(ts_ms, unit="ms", tz="UTC")
                rows.append({"timestamp": dt, "value": float(players)})
            return pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)

    _Collector.__name__ = f"Steam{name.title()}Collector"
    _Collector.__qualname__ = _Collector.__name__
    return _Collector


def get_steam_collectors() -> dict[str, type[BaseCollector]]:
    result = {}
    for app_id, name, display in STEAM_GAMES:
        cls = _make_steam_collector(app_id, name, display)
        result[name] = cls
    return result
