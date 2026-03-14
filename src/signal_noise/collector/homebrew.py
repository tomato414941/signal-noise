"""Homebrew formula install count collectors.

Tracks 30-day install counts for popular formulae as a proxy for
developer tooling adoption on macOS/Linux.

No API key required.  Docs: https://formulae.brew.sh/docs/api/
"""
from __future__ import annotations

import threading
import time

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_lock = threading.Lock()
_last_request: float = 0.0
_MIN_INTERVAL: float = 2.0


def _throttled_get(url: str, timeout: int = 30) -> dict:
    global _last_request
    with _lock:
        wait = _MIN_INTERVAL - (time.monotonic() - _last_request)
        if wait > 0:
            time.sleep(wait)
        resp = requests.get(url, timeout=timeout)
        _last_request = time.monotonic()
    resp.raise_for_status()
    return resp.json()


# (formula, collector_name, display_name)
_FORMULAE: list[tuple[str, str, str]] = [
    ("node", "brew_node", "Homebrew: node"),
    ("python@3.12", "brew_python", "Homebrew: python"),
    ("go", "brew_go", "Homebrew: go"),
    ("rust", "brew_rust", "Homebrew: rust"),
    ("postgresql@17", "brew_postgres", "Homebrew: postgresql"),
    ("redis", "brew_redis", "Homebrew: redis"),
    ("git", "brew_git", "Homebrew: git"),
    ("ffmpeg", "brew_ffmpeg", "Homebrew: ffmpeg"),
    ("docker", "brew_docker", "Homebrew: docker"),
    ("cmake", "brew_cmake", "Homebrew: cmake"),
    ("wget", "brew_wget", "Homebrew: wget"),
    ("ollama", "brew_ollama", "Homebrew: ollama"),
]


def _make_brew_collector(
    formula: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://formulae.brew.sh/docs/api/",
            domain="technology",
            category="developer",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://formulae.brew.sh/api/formula/{formula}.json"
            data = _throttled_get(url, timeout=self.config.request_timeout)
            installs_30d = data.get("analytics", {}).get("install_on_request", {}).get("30d", {})
            total = sum(v for v in installs_30d.values()) if isinstance(installs_30d, dict) else 0
            if total == 0:
                raise RuntimeError(f"No Homebrew install data for {formula}")
            now = pd.Timestamp.now(tz="UTC").floor("D")
            return pd.DataFrame([{"date": now, "value": float(total)}])

    _Collector.__name__ = f"Brew_{name}"
    _Collector.__qualname__ = f"Brew_{name}"
    return _Collector


def get_homebrew_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_brew_collector(formula, name, display)
        for formula, name, display in _FORMULAE
    }
