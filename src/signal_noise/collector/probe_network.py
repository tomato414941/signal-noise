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
    ("9.9.9.9", "quad9_dns", "Quad9 DNS"),
    ("208.67.222.222", "opendns", "OpenDNS"),
    ("8.26.56.26", "comodo_dns", "Comodo DNS"),
    ("94.140.14.14", "adguard_dns", "AdGuard DNS"),
    ("a.root-servers.net", "root_dns_a", "Root DNS A"),
    ("github.com", "github", "GitHub"),
    ("openai.com", "openai", "OpenAI"),
    ("wikipedia.org", "wikipedia", "Wikipedia"),
    ("baidu.com", "baidu", "Baidu"),
    ("google.co.jp", "google_jp", "Google JP"),
]

DNS_TARGETS: list[tuple[str, str, str]] = [
    ("google.com", "google", "Google"),
    ("cloudflare.com", "cloudflare", "Cloudflare"),
    ("github.com", "github", "GitHub"),
    ("openai.com", "openai", "OpenAI"),
    ("wikipedia.org", "wikipedia", "Wikipedia"),
    ("amazon.com", "amazon", "Amazon"),
    ("reddit.com", "reddit", "Reddit"),
    ("x.com", "x", "X"),
    ("apple.com", "apple", "Apple"),
    ("microsoft.com", "microsoft", "Microsoft"),
    ("baidu.com", "baidu", "Baidu"),
    ("ntp.org", "ntp", "NTP.org"),
]

HTTP_TARGETS: list[tuple[str, str, str]] = [
    ("https://www.google.com", "google", "Google"),
    ("https://github.com", "github", "GitHub"),
    ("https://www.cloudflare.com", "cloudflare", "Cloudflare"),
    ("https://openai.com", "openai", "OpenAI"),
    ("https://www.wikipedia.org", "wikipedia", "Wikipedia"),
    ("https://www.reddit.com", "reddit", "Reddit"),
    ("https://x.com", "x", "X"),
    ("https://www.apple.com", "apple", "Apple"),
    ("https://www.microsoft.com", "microsoft", "Microsoft"),
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


def _ping_samples_ms(host: str, count: int = 5, timeout: int = 5) -> list[float]:
    """Return per-packet ping RTT samples in ms."""
    try:
        result = subprocess.run(
            ["ping", "-c", str(count), "-W", str(timeout), host],
            capture_output=True, text=True, timeout=timeout + 5,
        )
        if result.returncode != 0:
            return []
        samples: list[float] = []
        for line in result.stdout.splitlines():
            if "time=" not in line:
                continue
            try:
                value = line.split("time=")[1].split()[0]
                samples.append(float(value))
            except Exception:
                continue
        return samples
    except Exception:
        return []


def _make_ping_collector(
    host: str, key: str, display: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=f"probe_ping_{key}",
            display_name=f"Ping RTT: {display}",
            update_frequency="hourly",
            api_docs_url="",
            domain="technology",
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
            domain="technology",
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


def _make_http_collector(
    url: str, key: str, display: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=f"probe_http_{key}",
            display_name=f"HTTP Response: {display}",
            update_frequency="hourly",
            api_docs_url="",
            domain="technology",
            category="internet",
            collection_level="L5",
        )

        def fetch(self) -> pd.DataFrame:
            ms = _http_response_ms(url)
            ts = pd.Timestamp.now(tz="UTC").floor("5min")
            value = ms if ms is not None else float("nan")
            return pd.DataFrame({"timestamp": [ts], "value": [value]})

    _Collector.__name__ = f"ProbeHttp_{key}"
    _Collector.__qualname__ = f"ProbeHttp_{key}"
    return _Collector


class ProbeNetworkStateCollector(BaseCollector):
    """Discrete network health state.

    value:
    - 0: normal
    - 1: degraded
    - 2: down
    """

    meta = CollectorMeta(
        name="probe_network_state",
        display_name="Probe Network State",
        update_frequency="hourly",
        api_docs_url="",
        domain="technology",
        category="internet",
        collection_level="L5",
        signal_type="state",
    )

    def fetch(self) -> pd.DataFrame:
        checks = [
            ("google", "https://www.google.com"),
            ("github", "https://github.com"),
            ("cloudflare", "https://www.cloudflare.com"),
        ]
        latencies: dict[str, float | None] = {}
        for key, url in checks:
            latencies[key] = _http_response_ms(url)

        ok_values = [v for v in latencies.values() if v is not None]
        if not ok_values:
            state = 2.0
        elif len(ok_values) < len(checks) or max(ok_values) > 1500:
            state = 1.0
        else:
            state = 0.0

        payload = {
            "latency_ms": latencies,
            "ok_count": len(ok_values),
            "total_count": len(checks),
        }
        ts = pd.Timestamp.now(tz="UTC").floor("5min")
        return pd.DataFrame({"timestamp": [ts], "value": [state], "payload": [payload]})


class ProbePingDistributionCollector(BaseCollector):
    """Ping latency distribution summary for core DNS targets."""

    meta = CollectorMeta(
        name="probe_ping_distribution_core",
        display_name="Ping Distribution: Core DNS",
        update_frequency="hourly",
        api_docs_url="",
        domain="technology",
        category="internet",
        collection_level="L5",
        signal_type="distribution",
    )

    def fetch(self) -> pd.DataFrame:
        hosts = ["8.8.8.8", "1.1.1.1", "9.9.9.9"]
        samples: list[float] = []
        for host in hosts:
            samples.extend(_ping_samples_ms(host, count=3, timeout=5))

        ts = pd.Timestamp.now(tz="UTC").floor("5min")
        if not samples:
            payload = {
                "n": 0,
                "p50": None,
                "p90": None,
                "p99": None,
                "mean": None,
                "std": None,
            }
            return pd.DataFrame({"timestamp": [ts], "value": [float("nan")], "payload": [payload]})

        s = pd.Series(samples, dtype="float64")
        p50 = float(s.quantile(0.50))
        p90 = float(s.quantile(0.90))
        p99 = float(s.quantile(0.99))
        mean = float(s.mean())
        std = float(s.std(ddof=0))
        payload = {
            "n": int(s.size),
            "p50": p50,
            "p90": p90,
            "p99": p99,
            "mean": mean,
            "std": std,
        }
        return pd.DataFrame({"timestamp": [ts], "value": [p90], "payload": [payload]})


class ProbeDnsStateCollector(BaseCollector):
    """Discrete DNS health state for core domains.

    value:
    - 0: normal
    - 1: degraded
    - 2: down
    """

    meta = CollectorMeta(
        name="probe_dns_state",
        display_name="Probe DNS State",
        update_frequency="hourly",
        api_docs_url="",
        domain="technology",
        category="internet",
        collection_level="L5",
        signal_type="state",
    )

    def fetch(self) -> pd.DataFrame:
        hosts = ["google.com", "github.com", "cloudflare.com", "openai.com"]
        resolves: dict[str, float | None] = {}
        for host in hosts:
            resolves[host] = _dns_resolve_ms(host)

        ok_values = [v for v in resolves.values() if v is not None]
        if not ok_values:
            state = 2.0
        elif len(ok_values) < len(hosts) or max(ok_values) > 1200:
            state = 1.0
        else:
            state = 0.0

        payload = {
            "resolve_ms": resolves,
            "ok_count": len(ok_values),
            "total_count": len(hosts),
        }
        ts = pd.Timestamp.now(tz="UTC").floor("5min")
        return pd.DataFrame({"timestamp": [ts], "value": [state], "payload": [payload]})


class ProbeHttpDistributionCollector(BaseCollector):
    """HTTP latency distribution summary for major web targets."""

    meta = CollectorMeta(
        name="probe_http_distribution_core",
        display_name="HTTP Distribution: Core Web Targets",
        update_frequency="hourly",
        api_docs_url="",
        domain="technology",
        category="internet",
        collection_level="L5",
        signal_type="distribution",
    )

    def fetch(self) -> pd.DataFrame:
        urls = [
            "https://www.google.com",
            "https://github.com",
            "https://www.cloudflare.com",
            "https://openai.com",
            "https://www.wikipedia.org",
        ]
        values = [_http_response_ms(url) for url in urls]
        samples = [v for v in values if v is not None]
        ts = pd.Timestamp.now(tz="UTC").floor("5min")
        if not samples:
            payload = {
                "n": 0,
                "p50": None,
                "p90": None,
                "p99": None,
                "mean": None,
                "std": None,
                "ok_count": 0,
                "total_count": len(urls),
            }
            return pd.DataFrame({"timestamp": [ts], "value": [float("nan")], "payload": [payload]})

        s = pd.Series(samples, dtype="float64")
        p50 = float(s.quantile(0.50))
        p90 = float(s.quantile(0.90))
        p99 = float(s.quantile(0.99))
        mean = float(s.mean())
        std = float(s.std(ddof=0))
        payload = {
            "n": int(s.size),
            "p50": p50,
            "p90": p90,
            "p99": p99,
            "mean": mean,
            "std": std,
            "ok_count": len(samples),
            "total_count": len(urls),
        }
        return pd.DataFrame({"timestamp": [ts], "value": [p90], "payload": [payload]})


def get_probe_collectors() -> dict[str, type[BaseCollector]]:
    out: dict[str, type[BaseCollector]] = {}
    for host, key, display in PING_TARGETS:
        name = f"probe_ping_{key}"
        out[name] = _make_ping_collector(host, key, display)
    for hostname, key, display in DNS_TARGETS:
        name = f"probe_dns_{key}"
        out[name] = _make_dns_collector(hostname, key, display)
    for url, key, display in HTTP_TARGETS:
        name = f"probe_http_{key}"
        out[name] = _make_http_collector(url, key, display)
    out[ProbeNetworkStateCollector.meta.name] = ProbeNetworkStateCollector
    out[ProbePingDistributionCollector.meta.name] = ProbePingDistributionCollector
    out[ProbeDnsStateCollector.meta.name] = ProbeDnsStateCollector
    out[ProbeHttpDistributionCollector.meta.name] = ProbeHttpDistributionCollector
    return out
