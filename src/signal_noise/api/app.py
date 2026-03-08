from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel

from signal_noise.api.ops_ui import render_ops_page
from signal_noise.config import DB_PATH
from signal_noise.store.event_bus import EventBus
from signal_noise.store.sqlite_store import SignalStore

log = logging.getLogger(__name__)

app = FastAPI(title="signal-noise", version="0.2.0")
_store: SignalStore | None = None
_event_bus: EventBus | None = None


def get_store() -> SignalStore:
    global _store
    if _store is None:
        _store = SignalStore(DB_PATH)
    return _store


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/ops", status_code=307)


@app.get("/ops", include_in_schema=False)
def ops_board() -> HTMLResponse:
    return HTMLResponse(render_ops_page())


@app.get("/health")
def health() -> dict:
    store = get_store()
    h = store.check_health()
    status = "ok"
    if h["failing"] or h["stale"]:
        status = "degraded"
    return {
        "status": status,
        "fresh": len(h["fresh"]),
        "stale": len(h["stale"]),
        "failing": len(h["failing"]),
        "never_seen": len(h["never_seen"]),
        "suppressed": len(h["suppressed"]),
    }


@app.get("/health/signals")
def health_signals() -> dict:
    store = get_store()
    h = store.check_health()
    return {
        "fresh": len(h["fresh"]),
        "suppressed": [
            {
                "name": s["name"],
                "reason": s.get("suppressed_reason"),
                "detail": s.get("suppressed_detail"),
                "scope": s.get("suppressed_scope"),
                "review_after": s.get("suppressed_review_after"),
                "source": s.get("suppressed_source"),
                "suppressed_at": s.get("suppressed_at"),
            }
            for s in h["suppressed"]
        ],
        "stale": [{"name": s["name"], "age_seconds": s["age_seconds"], "interval": s["interval"]} for s in h["stale"]],
        "failing": [
            {
                "name": s["name"],
                "consecutive_failures": s["consecutive_failures"],
                "error": s.get("last_error"),
                "error_at": s.get("last_error_at"),
            }
            for s in h["failing"]
        ],
        "never_seen": [s["name"] for s in h["never_seen"]],
    }


@app.get("/signals")
def list_signals(
    domain: str | None = Query(None),
    category: str | None = Query(None),
    signal_type: str | None = Query(None),
) -> list[dict]:
    signals = get_store().list_signals()
    if domain:
        signals = [s for s in signals if s.get("domain") == domain]
    if category:
        signals = [s for s in signals if s.get("category") == category]
    if signal_type:
        signals = [s for s in signals if s.get("signal_type") == signal_type]
    return signals


@app.get("/signals/{name}")
def signal_meta(name: str) -> dict:
    meta = get_store().get_meta(name)
    if not meta:
        raise HTTPException(404, f"Signal not found: {name}")
    return meta


@app.get("/signals/{name}/data")
def signal_data(
    name: str,
    since: str | None = Query(None),
    columns: str | None = Query(None),
    resolution: str | None = Query(None, pattern="^(1m|5m|1h|4h|1d)$"),
) -> list[dict]:
    store = get_store()
    meta = store.get_meta(name)
    if not meta:
        raise HTTPException(404, f"Signal not found: {name}")
    col_list = columns.split(",") if columns else None
    df = store.get_data(name, since=since, columns=col_list, resolution=resolution)
    # Fallback to realtime table for microstructure signals
    if df.empty and meta.get("category") == "microstructure":
        df = store.get_realtime_data(name, since=since)
    return df.to_dict(orient="records")


@app.get("/signals/{name}/latest")
def signal_latest(name: str) -> dict:
    latest = get_store().get_latest(name)
    if not latest:
        raise HTTPException(404, f"Signal not found: {name}")
    return latest


@app.get("/signals/{name}/realtime")
def signal_realtime(
    name: str,
    since: str | None = Query(None),
) -> list[dict]:
    store = get_store()
    meta = store.get_meta(name)
    if not meta:
        raise HTTPException(404, f"Signal not found: {name}")
    df = store.get_realtime_data(name, since=since)
    return df.to_dict(orient="records")


@app.get("/signals/{name}/anomalies")
def signal_anomalies(name: str, lookback: int | None = Query(None)) -> dict:
    store = get_store()
    meta = store.get_meta(name)
    if not meta:
        raise HTTPException(404, f"Signal not found: {name}")
    if lookback is None:
        interval_days = max(meta["interval"] // 86400, 1)
        lookback = max(100, interval_days * 30)
    # Check most recent data point against history
    df = store.get_data(name)
    if df.empty:
        return {"name": name, "anomalies": []}
    recent = df.tail(1)
    anomalies = store.check_anomalies(name, recent, lookback=lookback)
    return {"name": name, "anomalies": anomalies}


@app.get("/audit")
def audit_log(name: str | None = Query(None), limit: int = Query(100)) -> list[dict]:
    return get_store().get_audit_log(name=name, limit=limit)


class BatchRequest(BaseModel):
    names: list[str]
    since: str | None = None
    columns: list[str] | None = None
    resolution: str | None = None


@app.post("/signals/batch")
def signal_batch(req: BatchRequest) -> dict[str, list[dict]]:
    store = get_store()
    col_list = req.columns if req.columns else None
    batch = store.get_batch_data(
        req.names, since=req.since, columns=col_list, resolution=req.resolution,
    )
    return {name: df.to_dict(orient="records") for name, df in batch.items()}


@app.get("/health/events")
def health_events() -> dict:
    if _event_bus is None:
        return {"status": "disabled", "subscribers": 0}
    return {"status": "active", "subscribers": _event_bus.subscriber_count()}


@app.websocket("/ws/signals")
async def ws_signals(websocket: WebSocket, names: str = "*"):
    """Push signal events to subscribers via WebSocket.

    Query param ``names``: comma-separated signal name patterns
    (fnmatch glob). Example: /ws/signals?names=funding_rate_btc,liq_*
    """
    if _event_bus is None:
        await websocket.close(code=1013, reason="EventBus not available")
        return

    await websocket.accept()
    try:
        async for event in _event_bus.subscribe(names):
            await websocket.send_json({
                "name": event.name,
                "timestamp": event.timestamp,
                "value": event.value,
                "event_type": event.event_type,
                "detail": event.detail,
            })
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("WebSocket error for pattern=%s", names)
