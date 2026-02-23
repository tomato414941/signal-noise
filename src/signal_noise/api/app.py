from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from signal_noise.config import DB_PATH
from signal_noise.store.sqlite_store import SignalStore

app = FastAPI(title="signal-noise", version="0.1.0")
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
def list_signals() -> list[dict]:
    return get_store().list_signals()


@app.get("/signals/{name}")
def signal_meta(name: str) -> dict:
    meta = get_store().get_meta(name)
    if not meta:
        raise HTTPException(404, f"Signal not found: {name}")
    return meta


@app.get("/signals/{name}/data")
def signal_data(name: str, since: str | None = Query(None)) -> list[dict]:
    store = get_store()
    meta = store.get_meta(name)
    if not meta:
        raise HTTPException(404, f"Signal not found: {name}")
    df = store.get_data(name, since=since)
    return df.to_dict(orient="records")


@app.get("/signals/{name}/latest")
def signal_latest(name: str) -> dict:
    latest = get_store().get_latest(name)
    if not latest:
        raise HTTPException(404, f"Signal not found: {name}")
    return latest
