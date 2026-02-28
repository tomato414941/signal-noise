"""L5 active probing — network latency and DNS resolution time.

signal-noise's own server becomes a sensor. No external API is consumed;
the measurement originates from our infrastructure (Hetzner, Nuremberg DE).

Snapshot signals — cannot be backfilled.
"""
from __future__ import annotations

import subprocess

import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (host, name_suffix, display_suffix)
PING_TARGETS: list[tuple[str, str, str]] = [
    ("8.8.8.8", "google_dns", "Google DNS"),
    ("1.1.1.1", "cloudflare_dns", "Cloudflare DNS"),
    ("a.root-servers.net", "root_dns_a", "Root DNS A"),
    ("baidu.com", "baidu", "Baidu"),
    ("google.co.jp", "google_jp", "Google JP"),
]

DNS_TARGETS: list[tuple[str, str, str]] = [
    ("google.com", "google", "Google"),
    ("cloudflare.com", "cloudflare", "Cloudflare"),
    ("github.com", "github", "GitHub"),
    ("baidu.com", "baidu", "Baidu"),
    ("ntp.org", "ntp", "NTP.org"),
]


def _ping_ms(host: str, count: int = 3, timeout: int = 5) -> float | None:
    """Return average ping RTT in ms, or None on failure."""
    try:
        result = subprocess.run(
            ["ping", "-c", str(count), "-W", str(timeout), host],
            capture_output=True, text=True, timeout=timeout + 5,
        )
        if result.returncode != 0:
            return None
        for line in result.stdout.splitlines():
            if "avg" in line:
                # rtt min/avg/max/mdev = 1.234/5.678/9.012/1.234 ms
                parts = line.split("=")[-1].strip().split("/")
                return float(parts[1])
    except Exception:
        pass
    return None


def _dns_resolve_ms(hostname: str) -> float | None:
    """Return DNS resolution time in ms using dig."""
    try:
        result = subprocess.run(
            ["dig", "+noall", "+stats", hostname],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return None
        for line in result.stdout.splitlines():
            if "Query time:" in line:
                # ;; Query time: 12 msec
                parts = line.split()
                idx = parts.index("time:")
                return float(parts[idx + 1])
    except Exception:
        pass
    return None


def _http_response_ms(url: str, timeout: int = 10) -> float | None:
    """Return HTTP response time in ms using time measurement."""
    try:
        result = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{time_total}",
             "--max-time", str(timeout), url],
            capture_output=True, text=True, timeout=timeout + 5,
        )
        if result.returncode != 0:
            return None
        return float(result.stdout.strip()) * 1000
    except Exception:
        pass
    return None


def _make_ping_collector(
    host: str, key: str, display: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=f"probe_ping_{key}",
            display_name=f"Ping RTT: {display}",
            update_frequency="hourly",
            api_docs_url="",
            domain="infrastructure",
            category="internet",
            collection_level="L5",
        )

        def fetch(self) -> pd.DataFrame:
            rtt = _ping_ms(host)
            ts = pd.Timestamp.now(tz="UTC").floor("5min")
            value = rtt if rtt is not None else float("nan")
            return pd.DataFrame({"timestamp": [ts], "value": [value]})

    _Collector.__name__ = f"ProbePing_{key}"
    _Collector.__qualname__ = f"ProbePing_{key}"
    return _Collector


def _make_dns_collector(
    hostname: str, key: str, display: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=f"probe_dns_{key}",
            display_name=f"DNS Resolve: {display}",
            update_frequency="hourly",
            api_docs_url="",
            domain="infrastructure",
            category="internet",
            collection_level="L5",
        )

        def fetch(self) -> pd.DataFrame:
            ms = _dns_resolve_ms(hostname)
            ts = pd.Timestamp.now(tz="UTC").floor("5min")
            value = ms if ms is not None else float("nan")
            return pd.DataFrame({"timestamp": [ts], "value": [value]})

    _Collector.__name__ = f"ProbeDns_{key}"
    _Collector.__qualname__ = f"ProbeDns_{key}"
    return _Collector


class ProbeHTTPGoogleCollector(BaseCollector):
    """HTTP response time to google.com (ms)."""

    meta = CollectorMeta(
        name="probe_http_google",
        display_name="HTTP Response: Google",
        update_frequency="hourly",
        api_docs_url="",
        domain="infrastructure",
        category="internet",
        collection_level="L5",
    )

    def fetch(self) -> pd.DataFrame:
        ms = _http_response_ms("https://www.google.com")
        ts = pd.Timestamp.now(tz="UTC").floor("5min")
        value = ms if ms is not None else float("nan")
        return pd.DataFrame({"timestamp": [ts], "value": [value]})


class ProbeHTTPGitHubCollector(BaseCollector):
    """HTTP response time to github.com (ms)."""

    meta = CollectorMeta(
        name="probe_http_github",
        display_name="HTTP Response: GitHub",
        update_frequency="hourly",
        api_docs_url="",
        domain="infrastructure",
        category="internet",
        collection_level="L5",
    )

    def fetch(self) -> pd.DataFrame:
        ms = _http_response_ms("https://github.com")
        ts = pd.Timestamp.now(tz="UTC").floor("5min")
        value = ms if ms is not None else float("nan")
        return pd.DataFrame({"timestamp": [ts], "value": [value]})


class ProbeHTTPCloudflareCollector(BaseCollector):
    """HTTP response time to cloudflare.com (ms)."""

    meta = CollectorMeta(
        name="probe_http_cloudflare",
        display_name="HTTP Response: Cloudflare",
        update_frequency="hourly",
        api_docs_url="",
        domain="infrastructure",
        category="internet",
        collection_level="L5",
    )

    def fetch(self) -> pd.DataFrame:
        ms = _http_response_ms("https://www.cloudflare.com")
        ts = pd.Timestamp.now(tz="UTC").floor("5min")
        value = ms if ms is not None else float("nan")
        return pd.DataFrame({"timestamp": [ts], "value": [value]})


def get_probe_collectors() -> dict[str, type[BaseCollector]]:
    out: dict[str, type[BaseCollector]] = {}
    for host, key, display in PING_TARGETS:
        name = f"probe_ping_{key}"
        out[name] = _make_ping_collector(host, key, display)
    for hostname, key, display in DNS_TARGETS:
        name = f"probe_dns_{key}"
        out[name] = _make_dns_collector(hostname, key, display)
    return out
