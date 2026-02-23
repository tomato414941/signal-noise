from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


class SignalStore:
    """SQLite-backed storage for time-series signals (WAL mode)."""

    def __init__(self, db_path: Path | str, *, check_same_thread: bool = False):
        self._conn = sqlite3.connect(str(db_path), check_same_thread=check_same_thread)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()

    def _create_tables(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS signals (
                name      TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                value     REAL,
                PRIMARY KEY (name, timestamp)
            );
            CREATE TABLE IF NOT EXISTS signal_meta (
                name      TEXT PRIMARY KEY,
                domain    TEXT NOT NULL DEFAULT '',
                category  TEXT NOT NULL DEFAULT '',
                interval  INTEGER NOT NULL DEFAULT 86400,
                last_updated TEXT
            );
        """)

    def save(self, name: str, df: pd.DataFrame) -> None:
        if df.empty:
            return
        ts_col = "timestamp" if "timestamp" in df.columns else "date"
        val_col = "value" if "value" in df.columns else df.columns[-1]
        rows = [
            (name, str(row[ts_col]), float(row[val_col]) if pd.notna(row[val_col]) else None)
            for _, row in df.iterrows()
        ]
        self._conn.executemany(
            "INSERT OR REPLACE INTO signals (name, timestamp, value) VALUES (?, ?, ?)",
            rows,
        )
        self._conn.execute(
            "UPDATE signal_meta SET last_updated = datetime('now') WHERE name = ?",
            (name,),
        )
        self._conn.commit()

    def save_meta(self, name: str, domain: str, category: str, interval: int) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO signal_meta (name, domain, category, interval, last_updated)
               VALUES (?, ?, ?, ?, datetime('now'))""",
            (name, domain, category, interval),
        )
        self._conn.commit()

    def get_data(self, name: str, *, since: str | None = None) -> pd.DataFrame:
        if since:
            rows = self._conn.execute(
                "SELECT timestamp, value FROM signals WHERE name = ? AND timestamp >= ? ORDER BY timestamp",
                (name, since),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT timestamp, value FROM signals WHERE name = ? ORDER BY timestamp",
                (name,),
            ).fetchall()
        if not rows:
            return pd.DataFrame(columns=["timestamp", "value"])
        return pd.DataFrame([dict(r) for r in rows])

    def get_latest(self, name: str) -> dict | None:
        row = self._conn.execute(
            "SELECT timestamp, value FROM signals WHERE name = ? ORDER BY timestamp DESC LIMIT 1",
            (name,),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def get_meta(self, name: str) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM signal_meta WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def list_signals(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM signal_meta ORDER BY name"
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        self._conn.close()
