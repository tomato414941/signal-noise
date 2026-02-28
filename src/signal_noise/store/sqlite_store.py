from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

_OHLCV_COLS = ("open", "high", "low", "volume")


def _normalize_ts(ts) -> str:
    """Normalize timestamp to ISO 8601 format (T-separator)."""
    if isinstance(ts, pd.Timestamp):
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.isoformat()
    s = str(ts)
    # "2025-02-24 00:00:00+00:00" or "2025-02-24 14:30:01.142929+00:00"
    if len(s) >= 19 and s[10] == " ":
        s = s[:10] + "T" + s[11:]
    return s


class SignalStore:
    """SQLite-backed storage for time-series signals (WAL mode)."""

    def __init__(self, db_path: Path | str, *, check_same_thread: bool = False):
        self._conn = sqlite3.connect(str(db_path), check_same_thread=check_same_thread)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
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
            CREATE TABLE IF NOT EXISTS audit_log (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                name      TEXT NOT NULL,
                event     TEXT NOT NULL,
                rows      INTEGER,
                detail    TEXT
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
        if "consecutive_failures" not in meta_cols:
            self._conn.execute(
                "ALTER TABLE signal_meta ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0"
            )

        # Normalize existing space-separated timestamps to ISO 8601 T-separator
        # Step 1: delete space-separated rows where a T-separated duplicate exists
        self._conn.execute(
            "DELETE FROM signals WHERE substr(timestamp, 11, 1) = ' '"
            " AND EXISTS (SELECT 1 FROM signals b"
            " WHERE b.name = signals.name"
            " AND b.timestamp = substr(signals.timestamp, 1, 10)"
            " || 'T' || substr(signals.timestamp, 12))"
        )
        # Step 2: convert remaining space-separated timestamps
        self._conn.execute(
            "UPDATE signals"
            " SET timestamp = substr(timestamp, 1, 10) || 'T' || substr(timestamp, 12)"
            " WHERE length(timestamp) >= 19 AND substr(timestamp, 11, 1) = ' '"
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
            ts = _normalize_ts(row[ts_col])
            val = float(row[val_col]) if pd.notna(row[val_col]) else None
            if has_ohlcv:
                rows.append((
                    name, ts, val,
                    float(row["open"]) if pd.notna(row["open"]) else None,
                    float(row["high"]) if pd.notna(row["high"]) else None,
                    float(row["low"]) if pd.notna(row["low"]) else None,
                    float(row["volume"]) if pd.notna(row["volume"]) else None,
                ))
            else:
                rows.append((name, ts, val, None, None, None, None))

        self._conn.executemany(
            "INSERT OR REPLACE INTO signals (name, timestamp, value, open, high, low, volume)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"
            " WHERE name = ?",
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
               VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))""",
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
            params.append(_normalize_ts(since))

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

    def check_freshness(self, threshold_factor: float = 2.0) -> list[dict]:
        """Return signals that haven't been updated within threshold_factor * interval."""
        rows = self._conn.execute("""
            SELECT name, signal_type, interval, last_updated, consecutive_failures,
                   CAST((julianday('now') - julianday(last_updated)) * 86400 AS INTEGER)
                       AS age_seconds
            FROM signal_meta
            WHERE last_updated IS NOT NULL
        """).fetchall()
        stale = []
        for r in rows:
            d = dict(r)
            if d["age_seconds"] > d["interval"] * threshold_factor:
                d["expected_interval"] = d["interval"]
                stale.append(d)
        return stale

    def check_anomalies(
        self, name: str, df: pd.DataFrame, *, z_threshold: float = 4.0,
        lookback: int = 100,
    ) -> list[dict]:
        """Detect outliers in new data against recent history using z-scores.

        Returns list of anomaly dicts with timestamp, value, z_score, mean, std.
        Does NOT modify data — caller decides whether to save.
        """
        val_col = "value" if "value" in df.columns else df.columns[-1]
        new_values = df[val_col].dropna()
        if new_values.empty:
            return []

        # Fetch recent history for comparison
        rows = self._conn.execute(
            "SELECT value FROM signals WHERE name = ? AND value IS NOT NULL"
            " ORDER BY timestamp DESC LIMIT ?",
            (name, lookback),
        ).fetchall()
        if len(rows) < 10:
            return []  # Not enough history to judge

        hist = pd.Series([r[0] for r in rows], dtype=float)
        mean = hist.mean()
        std = hist.std()
        if std == 0 or pd.isna(std):
            return []

        ts_col = "timestamp" if "timestamp" in df.columns else "date"
        anomalies = []
        for idx, val in new_values.items():
            z = abs((val - mean) / std)
            if z > z_threshold:
                ts = str(df[ts_col].loc[idx]) if ts_col in df.columns else ""
                anomalies.append({
                    "timestamp": ts,
                    "value": float(val),
                    "z_score": round(float(z), 2),
                    "mean": round(float(mean), 4),
                    "std": round(float(std), 4),
                })
        return anomalies

    def reset_failures(self, name: str) -> None:
        self._conn.execute(
            "UPDATE signal_meta SET consecutive_failures = 0 WHERE name = ?",
            (name,),
        )
        self._conn.commit()

    def increment_failures(self, name: str) -> None:
        self._conn.execute(
            "UPDATE signal_meta SET consecutive_failures = consecutive_failures + 1"
            " WHERE name = ?",
            (name,),
        )
        self._conn.commit()

    def log_event(self, name: str, event: str, rows: int = 0, detail: str = "") -> None:
        self._conn.execute(
            "INSERT INTO audit_log (name, event, rows, detail) VALUES (?, ?, ?, ?)",
            (name, event, rows, detail),
        )
        self._conn.commit()

    def get_audit_log(self, name: str | None = None, limit: int = 100) -> list[dict]:
        if name:
            rows = self._conn.execute(
                "SELECT * FROM audit_log WHERE name = ? ORDER BY id DESC LIMIT ?",
                (name, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        self._conn.close()
