"""Cloud outage/status collectors across major providers.

Sources:
- AWS Health dashboard history events JSON
- Google Cloud incidents JSON
- Azure status RSS feed
- Cloudflare statuspage API
- OpenAI statuspage API
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
import xml.etree.ElementTree as ET

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_cache = SharedAPICache(ttl=300)

_AWS_HISTORY_URL = "https://history-events-us-east-1-prod.s3.amazonaws.com/historyevents.json"
_GCP_INCIDENTS_URL = "https://status.cloud.google.com/incidents.json"
_AZURE_RSS_URL = "https://azure.status.microsoft/en-us/status/feed/"
_CLOUDFLARE_UNRESOLVED_URL = "https://www.cloudflarestatus.com/api/v2/incidents/unresolved.json"
_CLOUDFLARE_INCIDENTS_URL = "https://www.cloudflarestatus.com/api/v2/incidents.json"
_OPENAI_UNRESOLVED_URL = "https://status.openai.com/api/v2/incidents/unresolved.json"
_OPENAI_INCIDENTS_URL = "https://status.openai.com/api/v2/incidents.json"


def _get_json(url: str, timeout: int = 15):
    def _fetch():
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    return _cache.get_or_fetch(f"json:{url}", _fetch)


def _get_text(url: str, timeout: int = 15) -> str:
    def _fetch():
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    return _cache.get_or_fetch(f"text:{url}", _fetch)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def _aws_counts(now: datetime) -> tuple[int, int]:
    data = _get_json(_AWS_HISTORY_URL)
    if not isinstance(data, dict):
        return 0, 0
    day_ago = now - timedelta(days=1)
    active = 0
    recent = 0
    for events in data.values():
        if not isinstance(events, list):
            continue
        for e in events:
            status = str(e.get("status", ""))
            if status and status != "0":
                active += 1
            try:
                ts = datetime.fromtimestamp(int(str(e.get("date", "0"))), tz=UTC)
                if ts >= day_ago:
                    recent += 1
            except Exception:
                continue
    return active, recent


def _gcp_counts_and_durations(now: datetime) -> tuple[int, int, list[float]]:
    data = _get_json(_GCP_INCIDENTS_URL)
    if not isinstance(data, list):
        return 0, 0, []
    day_ago = now - timedelta(days=1)
    month_ago = now - timedelta(days=30)
    active = 0
    recent = 0
    durations: list[float] = []
    for e in data:
        begin = _parse_dt(e.get("begin"))
        end = _parse_dt(e.get("end"))
        if end is None:
            active += 1
        if begin and begin >= day_ago:
            recent += 1
        if begin and end and begin >= month_ago and end >= begin:
            durations.append((end - begin).total_seconds() / 3600.0)
    return active, recent, durations


def _statuspage_counts_and_durations(
    unresolved_url: str,
    incidents_url: str,
    now: datetime,
) -> tuple[int, int, list[float]]:
    unresolved = _get_json(unresolved_url)
    incidents = _get_json(incidents_url)
    unresolved_items = unresolved.get("incidents", []) if isinstance(unresolved, dict) else []
    all_items = incidents.get("incidents", []) if isinstance(incidents, dict) else []
    day_ago = now - timedelta(days=1)
    month_ago = now - timedelta(days=30)
    active = len(unresolved_items)
    recent = 0
    durations: list[float] = []
    for e in all_items:
        created = _parse_dt(e.get("created_at"))
        resolved = _parse_dt(e.get("resolved_at"))
        if created and created >= day_ago:
            recent += 1
        if created and resolved and created >= month_ago and resolved >= created:
            durations.append((resolved - created).total_seconds() / 3600.0)
    return active, recent, durations


def _azure_counts(now: datetime) -> tuple[int, int]:
    xml_text = _get_text(_AZURE_RSS_URL)
    root = ET.fromstring(xml_text)
    items = root.findall(".//item")
    day_ago = now - timedelta(days=1)
    active = 0
    recent = 0
    for item in items:
        title = (item.findtext("title") or "").lower()
        desc = (item.findtext("description") or "").lower()
        pub = item.findtext("pubDate")
        ts = None
        if pub:
            try:
                ts = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=UTC)
            except Exception:
                ts = None
        if ts and ts >= day_ago:
            recent += 1
        # RSS has no strict status field; infer from common status words.
        text = f"{title} {desc}"
        if any(k in text for k in ("investigating", "degraded", "outage", "service issue")):
            active += 1
    return active, recent


def _compute_provider_snapshot() -> dict:
    now = datetime.now(UTC)
    out = {
        "aws": {"active": 0, "recent_24h": 0, "durations_h": []},
        "gcp": {"active": 0, "recent_24h": 0, "durations_h": []},
        "azure": {"active": 0, "recent_24h": 0, "durations_h": []},
        "cloudflare": {"active": 0, "recent_24h": 0, "durations_h": []},
        "openai": {"active": 0, "recent_24h": 0, "durations_h": []},
    }
    try:
        a_active, a_recent = _aws_counts(now)
        out["aws"]["active"] = a_active
        out["aws"]["recent_24h"] = a_recent
    except Exception:
        pass
    try:
        g_active, g_recent, g_dur = _gcp_counts_and_durations(now)
        out["gcp"]["active"] = g_active
        out["gcp"]["recent_24h"] = g_recent
        out["gcp"]["durations_h"] = g_dur
    except Exception:
        pass
    try:
        z_active, z_recent = _azure_counts(now)
        out["azure"]["active"] = z_active
        out["azure"]["recent_24h"] = z_recent
    except Exception:
        pass
    try:
        c_active, c_recent, c_dur = _statuspage_counts_and_durations(
            _CLOUDFLARE_UNRESOLVED_URL, _CLOUDFLARE_INCIDENTS_URL, now,
        )
        out["cloudflare"]["active"] = c_active
        out["cloudflare"]["recent_24h"] = c_recent
        out["cloudflare"]["durations_h"] = c_dur
    except Exception:
        pass
    try:
        o_active, o_recent, o_dur = _statuspage_counts_and_durations(
            _OPENAI_UNRESOLVED_URL, _OPENAI_INCIDENTS_URL, now,
        )
        out["openai"]["active"] = o_active
        out["openai"]["recent_24h"] = o_recent
        out["openai"]["durations_h"] = o_dur
    except Exception:
        pass
    return out


class CloudMajorStateCollector(BaseCollector):
    """Major cloud outage state across AWS/GCP/Azure/Cloudflare/OpenAI.

    value:
    - 0: normal (no active incidents)
    - 1: degraded (1-2 active incidents)
    - 2: severe (>=3 active incidents)
    """

    meta = CollectorMeta(
        name="cloud_major_state",
        display_name="Cloud Major Outage State",
        update_frequency="hourly",
        api_docs_url="https://status.cloud.google.com/",
        domain="technology",
        category="internet",
        signal_type="state",
        collect_interval=900,
    )

    def fetch(self) -> pd.DataFrame:
        snapshot = _compute_provider_snapshot()
        active_total = sum(int(v.get("active", 0)) for v in snapshot.values())
        if active_total == 0:
            state = 0.0
        elif active_total <= 2:
            state = 1.0
        else:
            state = 2.0
        ts = pd.Timestamp.now(tz="UTC").floor("15min")
        return pd.DataFrame({"timestamp": [ts], "value": [state], "payload": [snapshot]})


class CloudIncidentCount24hCollector(BaseCollector):
    """Count of cloud incidents created/reported in the last 24 hours."""

    meta = CollectorMeta(
        name="cloud_incident_count_24h",
        display_name="Cloud Incident Count (24h)",
        update_frequency="hourly",
        api_docs_url="https://status.cloud.google.com/",
        domain="technology",
        category="internet",
        signal_type="scalar",
        collect_interval=900,
    )

    def fetch(self) -> pd.DataFrame:
        snapshot = _compute_provider_snapshot()
        total = sum(int(v.get("recent_24h", 0)) for v in snapshot.values())
        ts = pd.Timestamp.now(tz="UTC").floor("15min")
        return pd.DataFrame({"timestamp": [ts], "value": [float(total)], "payload": [snapshot]})


class CloudRecoveryTimeDistributionCollector(BaseCollector):
    """Recent cloud incident recovery-time distribution (hours)."""

    meta = CollectorMeta(
        name="cloud_recovery_time_distribution",
        display_name="Cloud Recovery Time Distribution",
        update_frequency="hourly",
        api_docs_url="https://status.cloud.google.com/",
        domain="technology",
        category="internet",
        signal_type="distribution",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        snapshot = _compute_provider_snapshot()
        durations: list[float] = []
        for provider in snapshot.values():
            vals = provider.get("durations_h", [])
            if isinstance(vals, list):
                durations.extend([float(v) for v in vals if v is not None])
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        if not durations:
            payload = {"n": 0, "p50_h": None, "p90_h": None, "p99_h": None, "mean_h": None}
            return pd.DataFrame({"timestamp": [ts], "value": [float("nan")], "payload": [payload]})
        s = pd.Series(durations, dtype="float64")
        p50 = float(s.quantile(0.50))
        p90 = float(s.quantile(0.90))
        p99 = float(s.quantile(0.99))
        mean = float(s.mean())
        payload = {"n": int(s.size), "p50_h": p50, "p90_h": p90, "p99_h": p99, "mean_h": mean}
        return pd.DataFrame({"timestamp": [ts], "value": [p90], "payload": [payload]})

