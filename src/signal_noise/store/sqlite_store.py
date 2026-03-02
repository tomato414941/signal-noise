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
            CREATE TABLE IF NOT EXISTS signals_realtime (
                name      TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                value     REAL,
                PRIMARY KEY (name, timestamp)
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

    def _build_rows(self, name: str, df: pd.DataFrame) -> list[tuple]:
        ts_col = "timestamp" if "timestamp" in df.columns else "date"
        val_col = "value" if "value" in df.columns else df.columns[-1]
        has_ohlcv = all(c in df.columns for c in _OHLCV_COLS)
        rows = []
        for row in df.itertuples(index=False):
            ts = _normalize_ts(getattr(row, ts_col))
            val = float(getattr(row, val_col)) if pd.notna(getattr(row, val_col)) else None
            if has_ohlcv:
                rows.append((
                    name, ts, val,
                    float(row.open) if pd.notna(row.open) else None,
                    float(row.high) if pd.notna(row.high) else None,
                    float(row.low) if pd.notna(row.low) else None,
                    float(row.volume) if pd.notna(row.volume) else None,
                ))
            else:
                rows.append((name, ts, val, None, None, None, None))
        return rows

    def save(self, name: str, df: pd.DataFrame) -> int:
        """Save time-series data. Returns number of rows written."""
        if df.empty:
            return 0
        rows = self._build_rows(name, df)

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
        return len(rows)

    def save_meta(
        self, name: str, domain: str, category: str, interval: int,
        signal_type: str = "scalar",
    ) -> None:
        """Upsert signal metadata without touching last_updated.

        last_updated is only set by save() when actual data is written.
        """
        self._conn.execute(
            """INSERT INTO signal_meta (name, domain, category, interval, signal_type)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET
                   domain = excluded.domain,
                   category = excluded.category,
                   interval = excluded.interval,
                   signal_type = excluded.signal_type""",
            (name, domain, category, interval, signal_type),
        )
        self._conn.commit()

    _RESOLUTION_MAP = {
        "1m": "1min",
        "5m": "5min",
        "1h": "1h",
        "4h": "4h",
        "1d": "1D",
    }

    def get_data(
        self, name: str, *, since: str | None = None,
        columns: list[str] | None = None,
        resolution: str | None = None,
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

        if resolution and not df.empty:
            df = self._resample(df, resolution)

        return df

    def _resample(self, df: pd.DataFrame, resolution: str) -> pd.DataFrame:
        freq = self._RESOLUTION_MAP.get(resolution)
        if freq is None:
            return df

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601")
        df = df.set_index("timestamp")

        agg: dict[str, str] = {}
        if "value" in df.columns:
            agg["value"] = "last"
        if "open" in df.columns:
            agg["open"] = "first"
        if "high" in df.columns:
            agg["high"] = "max"
        if "low" in df.columns:
            agg["low"] = "min"
        if "volume" in df.columns:
            agg["volume"] = "sum"

        if not agg:
            return df.reset_index()

        resampled = df.resample(freq).agg(agg).dropna(how="all")
        resampled = resampled.reset_index()
        resampled["timestamp"] = resampled["timestamp"].apply(
            lambda t: t.isoformat() if hasattr(t, "isoformat") else str(t)
        )
        return resampled

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

    def check_health(self, threshold_factor: float = 2.0) -> dict:
        """Classify all signals into 4 states: never_seen, fresh, stale, failing."""
        rows = self._conn.execute("""
            SELECT name, domain, category, signal_type, interval,
                   last_updated, consecutive_failures,
                   CASE WHEN last_updated IS NOT NULL THEN
                       CAST((julianday('now') - julianday(last_updated)) * 86400 AS INTEGER)
                   END AS age_seconds
            FROM signal_meta
        """).fetchall()
        result: dict[str, list[dict]] = {
            "never_seen": [], "fresh": [], "stale": [], "failing": [],
        }
        for r in rows:
            d = dict(r)
            if d["consecutive_failures"] > 0:
                result["failing"].append(d)
            elif d["last_updated"] is None:
                result["never_seen"].append(d)
            elif d["age_seconds"] > d["interval"] * threshold_factor:
                result["stale"].append(d)
            else:
                result["fresh"].append(d)
        return result

    def check_anomalies(
        self, name: str, df: pd.DataFrame, *, z_threshold: float = 4.0,
        lookback: int = 100,
    ) -> list[dict]:
        """Detect outliers in new data against recent history.

        Uses robust median/MAD instead of mean/std to prevent baseline
        contamination from prior outliers in the lookback window.
        Returns list of anomaly dicts. Does NOT modify data.
        """
        val_col = "value" if "value" in df.columns else df.columns[-1]
        new_values = df[val_col].dropna()
        if new_values.empty:
            return []

        # Fetch recent history for comparison, skipping the newest values
        # to prevent the values being tested from contaminating the baseline.
        skip = len(new_values)
        rows = self._conn.execute(
            "SELECT value FROM signals WHERE name = ? AND value IS NOT NULL"
            " ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            (name, lookback, skip),
        ).fetchall()
        if len(rows) < 10:
            return []  # Not enough history to judge

        hist = pd.Series([r[0] for r in rows], dtype=float)
        median = hist.median()
        mad = (hist - median).abs().median() * 1.4826  # MAD → std scale
        if mad == 0 or pd.isna(mad):
            return []

        ts_col = "timestamp" if "timestamp" in df.columns else "date"
        anomalies = []
        for idx, val in new_values.items():
            z = abs((val - median) / mad)
            if z > z_threshold:
                ts = str(df[ts_col].loc[idx]) if ts_col in df.columns else ""
                anomalies.append({
                    "timestamp": ts,
                    "value": float(val),
                    "z_score": round(float(z), 2),
                    "median": round(float(median), 4),
                    "mad": round(float(mad), 4),
                })
        return anomalies

    def save_collection_result(
        self, name: str, df: pd.DataFrame,
        domain: str, category: str, interval: int,
        signal_type: str = "scalar",
        event: str = "collected",
    ) -> int:
        """Batch save: data + meta + reset failures + audit in one transaction."""
        if df.empty:
            return 0
        rows = self._build_rows(name, df)

        self._conn.executemany(
            "INSERT OR REPLACE INTO signals (name, timestamp, value, open, high, low, volume)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self._conn.execute(
            """INSERT INTO signal_meta (name, domain, category, interval, signal_type)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET
                   domain = excluded.domain,
                   category = excluded.category,
                   interval = excluded.interval,
                   signal_type = excluded.signal_type""",
            (name, domain, category, interval, signal_type),
        )
        self._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),"
            " consecutive_failures = 0 WHERE name = ?",
            (name,),
        )
        self._conn.execute(
            "INSERT INTO audit_log (name, event, rows, detail) VALUES (?, ?, ?, ?)",
            (name, event, len(rows), ""),
        )
        self._conn.commit()
        return len(rows)

    def save_collection_failure(self, name: str, error_detail: str = "") -> None:
        """Batch failure: increment failures + audit in one transaction."""
        self._conn.execute(
            "UPDATE signal_meta SET consecutive_failures = consecutive_failures + 1"
            " WHERE name = ?",
            (name,),
        )
        self._conn.execute(
            "INSERT INTO audit_log (name, event, rows, detail) VALUES (?, ?, ?, ?)",
            (name, "failed", 0, error_detail),
        )
        self._conn.commit()

    def log_collection_event(self, name: str, event: str, detail: str = "") -> None:
        """Single audit log entry (for circuit breaker events)."""
        self._conn.execute(
            "INSERT INTO audit_log (name, event, rows, detail) VALUES (?, ?, ?, ?)",
            (name, event, 0, detail),
        )
        self._conn.commit()

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

    def query_meta(
        self, *, interval: int | None = None, domain: str | None = None,
    ) -> list[dict]:
        """Query signal metadata with optional filters."""
        where_clauses: list[str] = []
        params: list = []
        if interval is not None:
            where_clauses.append("interval = ?")
            params.append(interval)
        if domain is not None:
            where_clauses.append("domain = ?")
            params.append(domain)

        sql = "SELECT * FROM signal_meta"
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY name"

        rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def get_signal_matrix(self, names: list[str]) -> pd.DataFrame:
        """Load daily date x value matrix for multiple signals (for analysis).

        Returns DataFrame with columns: name, date, value.
        """
        if not names:
            return pd.DataFrame(columns=["name", "date", "value"])
        placeholders = ",".join("?" for _ in names)
        rows = self._conn.execute(
            f"SELECT name, SUBSTR(timestamp, 1, 10) as date, value "
            f"FROM signals WHERE name IN ({placeholders})",
            names,
        ).fetchall()
        return pd.DataFrame([dict(r) for r in rows], columns=["name", "date", "value"])

    def get_values_since(self, name: str, since: str) -> list[dict]:
        """Get date+value pairs for a signal since a cutoff date."""
        rows = self._conn.execute(
            "SELECT SUBSTR(timestamp, 1, 10) as date, value "
            "FROM signals WHERE name = ? AND timestamp >= ? ORDER BY timestamp",
            (name, since),
        ).fetchall()
        return [dict(r) for r in rows]

    # ---- Realtime (1-min) signals ----

    def save_realtime(self, name: str, df: pd.DataFrame) -> int:
        """Save scalar time-series to signals_realtime. Returns rows written."""
        if df.empty:
            return 0
        ts_col = "timestamp" if "timestamp" in df.columns else "date"
        val_col = "value" if "value" in df.columns else df.columns[-1]
        rows = []
        for row in df.itertuples(index=False):
            ts = _normalize_ts(getattr(row, ts_col))
            val = float(getattr(row, val_col)) if pd.notna(getattr(row, val_col)) else None
            rows.append((name, ts, val))
        self._conn.executemany(
            "INSERT OR REPLACE INTO signals_realtime (name, timestamp, value)"
            " VALUES (?, ?, ?)",
            rows,
        )
        self._conn.commit()
        return len(rows)

    def save_realtime_collection_result(
        self, name: str, df: pd.DataFrame,
        domain: str, category: str, interval: int,
        signal_type: str = "scalar",
        event: str = "collected",
    ) -> int:
        """Batch save to signals_realtime + meta + audit."""
        if df.empty:
            return 0
        ts_col = "timestamp" if "timestamp" in df.columns else "date"
        val_col = "value" if "value" in df.columns else df.columns[-1]
        rows = []
        for row in df.itertuples(index=False):
            ts = _normalize_ts(getattr(row, ts_col))
            val = float(getattr(row, val_col)) if pd.notna(getattr(row, val_col)) else None
            rows.append((name, ts, val))
        self._conn.executemany(
            "INSERT OR REPLACE INTO signals_realtime (name, timestamp, value)"
            " VALUES (?, ?, ?)",
            rows,
        )
        self._conn.execute(
            """INSERT INTO signal_meta (name, domain, category, interval, signal_type)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET
                   domain = excluded.domain,
                   category = excluded.category,
                   interval = excluded.interval,
                   signal_type = excluded.signal_type""",
            (name, domain, category, interval, signal_type),
        )
        self._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),"
            " consecutive_failures = 0 WHERE name = ?",
            (name,),
        )
        self._conn.execute(
            "INSERT INTO audit_log (name, event, rows, detail) VALUES (?, ?, ?, ?)",
            (name, event, len(rows), ""),
        )
        self._conn.commit()
        return len(rows)

    def get_realtime_data(
        self, name: str, *, since: str | None = None,
    ) -> pd.DataFrame:
        """Query from signals_realtime."""
        params: list[str] = [name]
        where = "name = ?"
        if since:
            where += " AND timestamp >= ?"
            params.append(_normalize_ts(since))
        rows = self._conn.execute(
            f"SELECT timestamp, value FROM signals_realtime"
            f" WHERE {where} ORDER BY timestamp",
            params,
        ).fetchall()
        if not rows:
            return pd.DataFrame(columns=["timestamp", "value"])
        return pd.DataFrame([dict(r) for r in rows])

    def get_realtime_latest(self, name: str) -> dict | None:
        """Most recent value from signals_realtime."""
        row = self._conn.execute(
            "SELECT timestamp, value FROM signals_realtime"
            " WHERE name = ? ORDER BY timestamp DESC LIMIT 1",
            (name,),
        ).fetchone()
        return dict(row) if row else None

    def rollup_daily(self, name: str, agg: str = "mean") -> int:
        """Aggregate signals_realtime into daily values in the signals table."""
        agg_fn = {"mean": "AVG", "last": "MAX", "min": "MIN", "max": "MAX"}.get(agg, "AVG")
        rows = self._conn.execute(
            f"SELECT SUBSTR(timestamp, 1, 10) || 'T00:00:00+00:00' AS timestamp,"
            f" {agg_fn}(value) AS value"
            f" FROM signals_realtime WHERE name = ?"
            f" GROUP BY SUBSTR(timestamp, 1, 10)",
            (name,),
        ).fetchall()
        if not rows:
            return 0
        self._conn.executemany(
            "INSERT OR REPLACE INTO signals (name, timestamp, value, open, high, low, volume)"
            " VALUES (?, ?, ?, NULL, NULL, NULL, NULL)",
            [(name, r["timestamp"], r["value"]) for r in rows],
        )
        self._conn.commit()
        return len(rows)

    def purge_realtime(self, days: int = 30) -> int:
        """Delete signals_realtime rows older than N days."""
        cursor = self._conn.execute(
            "DELETE FROM signals_realtime"
            " WHERE timestamp < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', ?)",
            (f"-{days} days",),
        )
        self._conn.commit()
        return cursor.rowcount

    def close(self) -> None:
        self._conn.close()
