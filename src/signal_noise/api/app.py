from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from signal_noise.config import DB_PATH
from signal_noise.store.sqlite_store import SignalStore

app = FastAPI(title="signal-noise", version="0.2.0")
_store: SignalStore | None = None


def get_store() -> SignalStore:
    global _store
    if _store is None:
        _store = SignalStore(DB_PATH)
    return _store


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


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
) -> list[dict]:
    store = get_store()
    meta = store.get_meta(name)
    if not meta:
        raise HTTPException(404, f"Signal not found: {name}")
    col_list = columns.split(",") if columns else None
    df = store.get_data(name, since=since, columns=col_list)
    return df.to_dict(orient="records")


@app.get("/signals/{name}/latest")
def signal_latest(name: str) -> dict:
    latest = get_store().get_latest(name)
    if not latest:
        raise HTTPException(404, f"Signal not found: {name}")
    return latest


class BatchRequest(BaseModel):
    names: list[str]
    since: str | None = None
    columns: list[str] | None = None


@app.post("/signals/batch")
def signal_batch(req: BatchRequest) -> dict[str, list[dict]]:
    store = get_store()
    result: dict[str, list[dict]] = {}
    col_list = req.columns if req.columns else None
    for name in req.names:
        meta = store.get_meta(name)
        if not meta:
            continue
        df = store.get_data(name, since=req.since, columns=col_list)
        result[name] = df.to_dict(orient="records")
    return result
