"""Infra risk and outage-adjacent odd signals."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
import socket
import ssl
import subprocess
import xml.etree.ElementTree as ET

import pandas as pd
import requests

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector.base import BaseCollector, CollectorMeta

_cache = SharedAPICache(ttl=300)

_FAA_URL = "https://nasstatus.faa.gov/api/airport-status-information"
_UK_CARBON_URL = "https://api.carbonintensity.org.uk/intensity"
_RIPE_BGP_UPDATES = "https://stat.ripe.net/data/bgp-updates/data.json"


def _get_json(url: str, *, params: dict | None = None, timeout: int = 15):
    key = f"json:{url}:{params}"

    def _fetch():
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    return _cache.get_or_fetch(key, _fetch)


def _get_text(url: str, *, timeout: int = 15) -> str:
    key = f"text:{url}"

    def _fetch():
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.text

    return _cache.get_or_fetch(key, _fetch)


def _to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def _statuspage_incidents(url: str) -> list[dict]:
    data = _get_json(url)
    if not isinstance(data, dict):
        return []
    items = data.get("incidents", [])
    return items if isinstance(items, list) else []


def _quant_payload(values: list[float], *, suffix: str = "") -> dict:
    if not values:
        return {
            "n": 0,
            f"p50{suffix}": None,
            f"p90{suffix}": None,
            f"p99{suffix}": None,
            f"mean{suffix}": None,
            f"std{suffix}": None,
        }
    s = pd.Series(values, dtype="float64")
    return {
        "n": int(s.size),
        f"p50{suffix}": float(s.quantile(0.50)),
        f"p90{suffix}": float(s.quantile(0.90)),
        f"p99{suffix}": float(s.quantile(0.99)),
        f"mean{suffix}": float(s.mean()),
        f"std{suffix}": float(s.std(ddof=0)),
    }


def _http_ms(url: str, timeout: int = 10) -> float | None:
    try:
        result = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{time_total}", "--max-time", str(timeout), url],
            capture_output=True, text=True, timeout=timeout + 5,
        )
        if result.returncode != 0:
            return None
        return float(result.stdout.strip()) * 1000.0
    except Exception:
        return None


def _dnssec_validated(domain: str, resolver: str = "1.1.1.1") -> bool:
    try:
        result = subprocess.run(
            ["dig", f"@{resolver}", domain, "A", "+dnssec"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return False
        # `ad` flag implies authenticated data by validating resolver.
        for line in result.stdout.splitlines():
            if "flags:" in line:
                return " ad;" in line or " ad " in line
    except Exception:
        return False
    return False


def _tls_days_to_expiry(host: str, port: int = 443) -> float | None:
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                cert = ssock.getpeercert()
        not_after = cert.get("notAfter")
        if not not_after:
            return None
        dt = datetime.strptime(not_after, "%b %d %H:%M:%S %Y %Z").replace(tzinfo=UTC)
        return (dt - datetime.now(UTC)).total_seconds() / 86400.0
    except Exception:
        return None


def _faa_counts() -> tuple[int, int]:
    xml_text = _get_text(_FAA_URL)
    root = ET.fromstring(xml_text)
    delay_count = 0
    ground_stop_count = 0
    for delay_type in root.iter("Delay_type"):
        name_el = delay_type.find("Name")
        if name_el is None:
            continue
        name = (name_el.text or "").strip()
        if name == "Ground Delay Programs":
            delay_count += len(delay_type.findall(".//Ground_Delay"))
        elif name == "Ground Stop Programs":
            ground_stop_count += len(delay_type.findall(".//Program"))
        elif "Arrival" in name or "Departure" in name:
            delay_count += len(delay_type.findall(".//Delay"))
    return delay_count, ground_stop_count


def _recent_python_incidents_24h(now: datetime) -> int:
    rss = _get_text("https://status.python.org/history.rss")
    root = ET.fromstring(rss)
    day_ago = now - timedelta(days=1)
    n = 0
    for item in root.findall(".//item"):
        pub = item.findtext("pubDate")
        if not pub:
            continue
        try:
            dt = parsedate_to_datetime(pub).astimezone(UTC)
            if dt >= day_ago:
                n += 1
        except Exception:
            continue
    return n


def _recent_uk_carbon_values_24h(now: datetime) -> list[float]:
    start = now - timedelta(days=1)
    url = (
        f"{_UK_CARBON_URL}/{start.strftime('%Y-%m-%dT%H:%MZ')}"
        f"/{now.strftime('%Y-%m-%dT%H:%MZ')}"
    )
    data = _get_json(url)
    rows = data.get("data", []) if isinstance(data, dict) else []
    out: list[float] = []
    for row in rows:
        intensity = row.get("intensity", {})
        val = intensity.get("actual")
        if val is None:
            val = intensity.get("forecast")
        if val is None:
            continue
        try:
            out.append(float(val))
        except Exception:
            continue
    return out


def _bgp_updates_count(resource: str) -> int:
    data = _get_json(_RIPE_BGP_UPDATES, params={"resource": resource})
    inner = data.get("data", {}) if isinstance(data, dict) else {}
    val = inner.get("nr_updates", 0)
    try:
        return int(val)
    except Exception:
        return 0


class DNSSECValidationStateCollector(BaseCollector):
    meta = CollectorMeta(
        name="dnssec_validation_state",
        display_name="DNSSEC Validation State",
        update_frequency="hourly",
        api_docs_url="https://www.rfc-editor.org/rfc/rfc4033",
        domain="technology",
        category="internet",
        signal_type="state",
        collect_interval=900,
    )

    def fetch(self) -> pd.DataFrame:
        domains = ["cloudflare.com", "ietf.org", "nic.cz", "org"]
        results = {d: _dnssec_validated(d) for d in domains}
        ok = sum(1 for v in results.values() if v)
        if ok == len(domains):
            state = 0.0
        elif ok == 0:
            state = 2.0
        else:
            state = 1.0
        payload = {"validated": results, "ok_count": ok, "total_count": len(domains)}
        ts = pd.Timestamp.now(tz="UTC").floor("15min")
        return pd.DataFrame({"timestamp": [ts], "value": [state], "payload": [payload]})


class TLSExpiryRiskDistributionCollector(BaseCollector):
    meta = CollectorMeta(
        name="tls_expiry_risk_distribution",
        display_name="TLS Expiry Risk Distribution",
        update_frequency="daily",
        api_docs_url="https://datatracker.ietf.org/doc/html/rfc5280",
        domain="technology",
        category="internet",
        signal_type="distribution",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        hosts = [
            "www.google.com",
            "github.com",
            "www.cloudflare.com",
            "pypi.org",
            "aws.amazon.com",
            "status.openai.com",
        ]
        vals = [_tls_days_to_expiry(h) for h in hosts]
        samples = [v for v in vals if v is not None]
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        if not samples:
            payload = {"n": 0, "p10_days": None, "p50_days": None, "p90_days": None, "min_days": None}
            return pd.DataFrame({"timestamp": [ts], "value": [float("nan")], "payload": [payload]})
        s = pd.Series(samples, dtype="float64")
        payload = {
            "n": int(s.size),
            "p10_days": float(s.quantile(0.10)),
            "p50_days": float(s.quantile(0.50)),
            "p90_days": float(s.quantile(0.90)),
            "min_days": float(s.min()),
            "max_days": float(s.max()),
        }
        # Lower p10 means higher risk.
        return pd.DataFrame({"timestamp": [ts], "value": [payload["p10_days"]], "payload": [payload]})


class BGPPathInstabilityIndexCollector(BaseCollector):
    meta = CollectorMeta(
        name="bgp_path_instability_index",
        display_name="BGP Path Instability Index",
        update_frequency="hourly",
        api_docs_url="https://stat.ripe.net/docs/data_api#bgp-updates",
        domain="technology",
        category="internet",
        signal_type="distribution",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        resources = ["8.8.8.0/24", "1.1.1.0/24", "208.67.222.0/24", "9.9.9.0/24"]
        counts = [float(_bgp_updates_count(r)) for r in resources]
        payload = _quant_payload(counts)
        payload["resources"] = resources
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [payload["p90"]], "payload": [payload]})


class CDNDivergenceStateCollector(BaseCollector):
    meta = CollectorMeta(
        name="cdn_divergence_state",
        display_name="CDN Divergence State",
        update_frequency="hourly",
        api_docs_url="",
        domain="technology",
        category="internet",
        signal_type="state",
        collect_interval=900,
    )

    def fetch(self) -> pd.DataFrame:
        targets = {
            "cloudflare": "https://www.cloudflare.com",
            "fastly": "https://www.fastly.com",
            "akamai": "https://www.akamai.com",
        }
        lat = {k: _http_ms(v) for k, v in targets.items()}
        ok = [v for v in lat.values() if v is not None]
        if not ok:
            state = 2.0
        else:
            ratio = max(ok) / max(min(ok), 1.0)
            if len(ok) < len(targets) or ratio >= 3.0:
                state = 1.0
            else:
                state = 0.0
        payload = {"latency_ms": lat, "ok_count": len(ok), "total_count": len(targets)}
        ts = pd.Timestamp.now(tz="UTC").floor("15min")
        return pd.DataFrame({"timestamp": [ts], "value": [state], "payload": [payload]})


class APIErrorSurface24hCollector(BaseCollector):
    meta = CollectorMeta(
        name="api_error_surface_24h",
        display_name="API Error Surface (24h)",
        update_frequency="hourly",
        api_docs_url="https://www.githubstatus.com/api",
        domain="technology",
        category="internet",
        signal_type="distribution",
        collect_interval=900,
    )

    def fetch(self) -> pd.DataFrame:
        now = datetime.now(UTC)
        day_ago = now - timedelta(days=1)
        providers: dict[str, tuple[str, str]] = {
            "github": (
                "https://www.githubstatus.com/api/v2/incidents/unresolved.json",
                "https://www.githubstatus.com/api/v2/incidents.json",
            ),
            "cloudflare": (
                "https://www.cloudflarestatus.com/api/v2/incidents/unresolved.json",
                "https://www.cloudflarestatus.com/api/v2/incidents.json",
            ),
            "npm": (
                "https://status.npmjs.org/api/v2/incidents/unresolved.json",
                "https://status.npmjs.org/api/v2/incidents.json",
            ),
        }
        counts: list[float] = []
        by_provider: dict[str, dict] = {}
        for name, (unresolved_url, incidents_url) in providers.items():
            unresolved = _statuspage_incidents(unresolved_url)
            incidents = _statuspage_incidents(incidents_url)
            recent = 0
            for i in incidents:
                dt = _to_dt(i.get("created_at"))
                if dt and dt >= day_ago:
                    recent += 1
            total = float(len(unresolved) + recent)
            counts.append(total)
            by_provider[name] = {"active": len(unresolved), "recent_24h": recent, "total": total}
        payload = _quant_payload(counts)
        payload["providers"] = by_provider
        payload["total"] = float(sum(counts))
        ts = pd.Timestamp.now(tz="UTC").floor("15min")
        return pd.DataFrame({"timestamp": [ts], "value": [payload["p90"]], "payload": [payload]})


class DependencyBlastRadiusCollector(BaseCollector):
    meta = CollectorMeta(
        name="dependency_blast_radius",
        display_name="Dependency Blast Radius",
        update_frequency="hourly",
        api_docs_url="https://www.githubstatus.com/api",
        domain="technology",
        category="internet",
        signal_type="distribution",
        collect_interval=900,
    )

    def fetch(self) -> pd.DataFrame:
        providers = {
            "github": "https://www.githubstatus.com/api/v2/incidents/unresolved.json",
            "cloudflare": "https://www.cloudflarestatus.com/api/v2/incidents/unresolved.json",
            "npm": "https://status.npmjs.org/api/v2/incidents/unresolved.json",
            "gcp": "https://status.cloud.google.com/incidents.json",
        }
        active_counts: dict[str, int] = {}
        for name, url in providers.items():
            if name == "gcp":
                data = _get_json(url)
                active = 0
                if isinstance(data, list):
                    active = sum(1 for item in data if _to_dt(item.get("end")) is None)
            else:
                active = len(_statuspage_incidents(url))
            active_counts[name] = int(active)
        values = [float(v) for v in active_counts.values()]
        payload = _quant_payload(values)
        payload["providers"] = active_counts
        payload["providers_impacted"] = sum(1 for v in active_counts.values() if v > 0)
        ts = pd.Timestamp.now(tz="UTC").floor("15min")
        return pd.DataFrame({"timestamp": [ts], "value": [payload["p90"]], "payload": [payload]})


class PaymentRailStateCollector(BaseCollector):
    meta = CollectorMeta(
        name="payment_rail_state",
        display_name="Payment Rail State",
        update_frequency="hourly",
        api_docs_url="",
        domain="technology",
        category="internet",
        signal_type="state",
        collect_interval=900,
    )

    def fetch(self) -> pd.DataFrame:
        targets = {
            "stripe": "https://checkout.stripe.com",
            "paypal": "https://www.paypal.com",
            "square": "https://squareup.com",
        }
        lat = {k: _http_ms(v) for k, v in targets.items()}
        ok = [v for v in lat.values() if v is not None]
        if not ok:
            state = 2.0
        elif len(ok) < len(targets) or max(ok) > 2500:
            state = 1.0
        else:
            state = 0.0
        payload = {"latency_ms": lat, "ok_count": len(ok), "total_count": len(targets)}
        ts = pd.Timestamp.now(tz="UTC").floor("15min")
        return pd.DataFrame({"timestamp": [ts], "value": [state], "payload": [payload]})


class RepoSupplyChainAlertsCollector(BaseCollector):
    meta = CollectorMeta(
        name="repo_supply_chain_alerts",
        display_name="Repo Supply Chain Alerts",
        update_frequency="hourly",
        api_docs_url="https://status.python.org/history.rss",
        domain="technology",
        category="safety",
        signal_type="scalar",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        now = datetime.now(UTC)
        day_ago = now - timedelta(days=1)
        github_recent = 0
        npm_recent = 0
        for name, url in [
            ("github", "https://www.githubstatus.com/api/v2/incidents.json"),
            ("npm", "https://status.npmjs.org/api/v2/incidents.json"),
        ]:
            incidents = _statuspage_incidents(url)
            cnt = 0
            for i in incidents:
                dt = _to_dt(i.get("created_at"))
                if dt and dt >= day_ago:
                    cnt += 1
            if name == "github":
                github_recent = cnt
            else:
                npm_recent = cnt
        python_recent = _recent_python_incidents_24h(now)
        total = float(github_recent + npm_recent + python_recent)
        payload = {
            "github_recent_24h": github_recent,
            "npm_recent_24h": npm_recent,
            "python_recent_24h": python_recent,
            "total_recent_24h": total,
        }
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [total], "payload": [payload]})


class AirportDisruptionStateCollector(BaseCollector):
    meta = CollectorMeta(
        name="airport_disruption_state",
        display_name="Airport Disruption State",
        update_frequency="hourly",
        api_docs_url="https://nasstatus.faa.gov/api/airport-status-information",
        domain="technology",
        category="aviation",
        signal_type="state",
        collect_interval=900,
    )

    def fetch(self) -> pd.DataFrame:
        delay_count, ground_stop_count = _faa_counts()
        score = delay_count + (ground_stop_count * 5)
        if score < 10:
            state = 0.0
        elif score < 40:
            state = 1.0
        else:
            state = 2.0
        payload = {
            "delay_count": delay_count,
            "ground_stop_count": ground_stop_count,
            "score": score,
        }
        ts = pd.Timestamp.now(tz="UTC").floor("15min")
        return pd.DataFrame({"timestamp": [ts], "value": [state], "payload": [payload]})


class EnergyGridStressDistributionCollector(BaseCollector):
    meta = CollectorMeta(
        name="energy_grid_stress_distribution",
        display_name="Energy Grid Stress Distribution",
        update_frequency="hourly",
        api_docs_url="https://carbon-intensity.github.io/api-definitions/",
        domain="economy",
        category="energy",
        signal_type="distribution",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        now = datetime.now(UTC)
        vals = _recent_uk_carbon_values_24h(now)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        if not vals:
            payload = {"n": 0, "p50": None, "p90": None, "p99": None, "mean": None, "spike_count": 0}
            return pd.DataFrame({"timestamp": [ts], "value": [float("nan")], "payload": [payload]})
        s = pd.Series(vals, dtype="float64")
        payload = {
            "n": int(s.size),
            "p50": float(s.quantile(0.50)),
            "p90": float(s.quantile(0.90)),
            "p99": float(s.quantile(0.99)),
            "mean": float(s.mean()),
            "std": float(s.std(ddof=0)),
            "max": float(s.max()),
            "spike_count": int((s > 300).sum()),
        }
        # p90 carbon intensity as stress proxy.
        return pd.DataFrame({"timestamp": [ts], "value": [payload["p90"]], "payload": [payload]})

