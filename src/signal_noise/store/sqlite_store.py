from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

_OHLCV_COLS = ("open", "high", "low", "volume")


class SignalStore:
    """SQLite-backed storage for time-series signals (WAL mode)."""

    def __init__(self, db_path: Path | str, *, check_same_thread: bool = False):
        self._conn = sqlite3.connect(str(db_path), check_same_thread=check_same_thread)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()
        self._migrate()

    def _create_tables(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS signals (
                name      TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                value     REAL,
                open      REAL,
                high      REAL,
                low       REAL,
                volume    REAL,
                PRIMARY KEY (name, timestamp)
            );
            CREATE TABLE IF NOT EXISTS signal_meta (
                name         TEXT PRIMARY KEY,
                domain       TEXT NOT NULL DEFAULT '',
                category     TEXT NOT NULL DEFAULT '',
                interval     INTEGER NOT NULL DEFAULT 86400,
                signal_type  TEXT NOT NULL DEFAULT 'scalar',
                last_updated TEXT
            );
        """)

    def _migrate(self) -> None:
        """Add columns that may be missing from older databases."""
        sig_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(signals)")}
        for col in _OHLCV_COLS:
            if col not in sig_cols:
                self._conn.execute(f"ALTER TABLE signals ADD COLUMN {col} REAL")

        meta_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(signal_meta)")}
        if "signal_type" not in meta_cols:
            self._conn.execute(
                "ALTER TABLE signal_meta ADD COLUMN signal_type TEXT NOT NULL DEFAULT 'scalar'"
            )
        self._conn.commit()

    def save(self, name: str, df: pd.DataFrame) -> None:
        if df.empty:
            return
        ts_col = "timestamp" if "timestamp" in df.columns else "date"
        val_col = "value" if "value" in df.columns else df.columns[-1]
        has_ohlcv = all(c in df.columns for c in _OHLCV_COLS)

        rows = []
        for _, row in df.iterrows():
            val = float(row[val_col]) if pd.notna(row[val_col]) else None
            if has_ohlcv:
                rows.append((
                    name, str(row[ts_col]), val,
                    float(row["open"]) if pd.notna(row["open"]) else None,
                    float(row["high"]) if pd.notna(row["high"]) else None,
                    float(row["low"]) if pd.notna(row["low"]) else None,
                    float(row["volume"]) if pd.notna(row["volume"]) else None,
                ))
            else:
                rows.append((name, str(row[ts_col]), val, None, None, None, None))

        self._conn.executemany(
            "INSERT OR REPLACE INTO signals (name, timestamp, value, open, high, low, volume)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self._conn.execute(
            "UPDATE signal_meta SET last_updated = datetime('now') WHERE name = ?",
            (name,),
        )
        self._conn.commit()

    def save_meta(
        self, name: str, domain: str, category: str, interval: int,
        signal_type: str = "scalar",
    ) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO signal_meta
               (name, domain, category, interval, signal_type, last_updated)
               VALUES (?, ?, ?, ?, ?, datetime('now'))""",
            (name, domain, category, interval, signal_type),
        )
        self._conn.commit()

    def get_data(
        self, name: str, *, since: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        all_cols = ["timestamp", "value", "open", "high", "low", "volume"]
        if columns:
            selected = ["timestamp"] + [c for c in columns if c in all_cols and c != "timestamp"]
        else:
            selected = all_cols

        col_sql = ", ".join(selected)
        params: list[str] = [name]
        where = "name = ?"
        if since:
            where += " AND timestamp >= ?"
            params.append(since)

        rows = self._conn.execute(
            f"SELECT {col_sql} FROM signals WHERE {where} ORDER BY timestamp",
            params,
        ).fetchall()

        if not rows:
            return pd.DataFrame(columns=["timestamp", "value"])

        df = pd.DataFrame([dict(r) for r in rows])

        # Drop all-NULL OHLCV columns when no explicit column selection
        if not columns:
            for col in _OHLCV_COLS:
                if col in df.columns and df[col].isna().all():
                    df = df.drop(columns=[col])

        return df

    def get_latest(self, name: str) -> dict | None:
        row = self._conn.execute(
            "SELECT timestamp, value, open, high, low, volume"
            " FROM signals WHERE name = ? ORDER BY timestamp DESC LIMIT 1",
            (name,),
        ).fetchone()
        if row is None:
            return None
        result = dict(row)
        # Strip NULL OHLCV fields for scalar signals
        return {k: v for k, v in result.items() if v is not None or k in ("timestamp", "value")}

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
