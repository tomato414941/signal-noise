# signal-noise: Design Document

> This document captures architectural decisions from a design discussion (2026-02-23).
> It defines the future direction of signal-noise and its relationship with consumer projects.

## Core Principle

**signal-noise has no purpose.** It collects and delivers time series data without
any assumption about what the data will be used for. Prediction, evaluation,
transformation — all of these belong to the consumer.

> "even noise is worth collecting -- the signal hides within"

## Project Responsibility

signal-noise is a **data collection service**: collect worldwide time series and
deliver them in a unified format via API.

| Responsibility        | signal-noise | Consumer |
|-----------------------|:------------:|:--------:|
| Data collection       | ○            |          |
| Scheduling            | ○            |          |
| Unified storage       | ○            |          |
| REST API delivery     | ○            |          |
| Transforms            |              | ○        |
| Evaluation            |              | ○        |
| Prediction / Strategy |              | ○        |

### What to remove from signal-noise

The following modules exist today but belong to the consumer side:

- `evaluator/` — evaluation is purpose-dependent
- `reporter/` — reporting is purpose-dependent
- `transforms.py` — transforms are purpose-dependent

### What to add to signal-noise

- `scheduler/` — per-collector scheduling based on natural update frequency
- `api/` — REST API for data delivery
- `store/` — storage abstraction (currently raw Parquet files)

## Architecture (Future)

```
┌─ signal-noise service ─────────────────────────────┐
│                                                     │
│  Scheduler ──→ Collector ──→ Store ──→ REST API     │
│  (per-collector   (API        (SQLite)   (delivery  │
│   frequency)       fetch)                 to        │
│                                           consumers)│
└─────────────────────────────────────────────────────┘
         ↓
    Consumer projects (purpose-dependent)
```

## Data Contract

### What signal-noise delivers

- **Raw time series only**: ~1,067 collector outputs
- **Format**: `DataFrame[timestamp, value]` (or `[date, value]`)
- **No derived signals**: transforms (z-score, SMA, RSI, etc.) are not applied
- **Metadata**: name, domain, category, update frequency, last updated

### REST API (Draft)

Consumer pulls data via polling. Push notification is not needed — the fastest
collector updates hourly, so polling intervals of minutes are sufficient.

```
GET /signals                           # List all signals (name, domain, category, ...)
GET /signals/{name}                    # Signal metadata (domain, category, frequency, last_updated)
GET /signals/{name}/data?since=...     # Time series data (timestamp, value)
GET /signals/{name}/latest             # Latest value + timestamp
GET /health                            # Service health check
```

Consumer workflow:
1. On startup: `GET /signals` to discover available signals
2. For each signal: `GET /signals/{name}/data?since=<last_seen>` to backfill
3. Periodically: poll `/signals/{name}/latest` or `/data?since=...` for updates
4. Handle 404 / empty response gracefully (signal temporarily unavailable)

## Collector Scheduling

Each collector has a natural update frequency:

| Example              | Frequency        |
|----------------------|------------------|
| BTC OHLCV            | Hourly           |
| Fear & Greed Index   | Daily            |
| VIX                  | Daily (business) |
| FRED macro           | Monthly          |
| BLS employment       | Monthly          |
| Climate data         | Daily–Weekly     |

signal-noise runs as a long-lived service, scheduling each collector independently.

### Implementation: asyncio self-managed loop

No external scheduler dependency (APScheduler, Celery). Each collector runs as
an independent `asyncio` task with its own interval.

```python
async def run_collector(collector, store, interval_seconds):
    while True:
        try:
            data = await asyncio.to_thread(collector.fetch)
            store.save(collector.meta.name, data)
        except Exception:
            log.error("Collector %s failed", collector.meta.name)
        await asyncio.sleep(interval_seconds)
```

`CollectorMeta` gains an `interval` field:

```python
@dataclass
class CollectorMeta:
    name: str
    domain: str
    category: str
    interval: int  # seconds between fetches (3600=hourly, 86400=daily)
```

Properties:
- Zero external dependencies
- One collector failure does not affect others
- On service restart, all collectors run once immediately, then resume their interval

## Storage: SQLite

Parquet (current) is batch-oriented — append requires read-modify-write, range queries
require full file scan. SQLite fits the service access pattern naturally.

### Schema

```sql
CREATE TABLE signals (
    name       TEXT NOT NULL,
    timestamp  TEXT NOT NULL,
    value      REAL,
    PRIMARY KEY (name, timestamp)
);

CREATE TABLE signal_meta (
    name         TEXT PRIMARY KEY,
    domain       TEXT NOT NULL,
    category     TEXT NOT NULL,
    interval     INTEGER NOT NULL,
    last_updated TEXT
);
```

### Why SQLite

- **Append**: INSERT, no read-modify-write
- **Range query**: WHERE clause, no full scan
- **Concurrency**: WAL mode handles parallel scheduler writes + API reads
- **Dependency**: Python standard library, zero external deps
- **Data volume**: ~1,067 series × ~2,000 rows ≈ 34 MB — trivially small

### Migration

Existing Parquet files are imported once into SQLite on first service startup.
After migration, Parquet files are no longer the source of truth.

## Consumer Design

### Core Challenge

In production, the set of available signals is **dynamic**:

- Signals go missing (API failures, provider outages)
- New signals are added (new collectors deployed)
- Different signals update at different frequencies

The consumer must treat signal availability as **variable by default**, not as an exception.

### Architecture: Independent Scoring → Online Aggregation

```
signal-noise (service)
    ↓  raw data via REST API
Consumer
    ├── Ingestion:     Receive signals as they arrive
    ├── Transform:     Purpose-specific feature engineering
    ├── Scoring:       Each signal independently produces a prediction score
    ├── Weighting:     Online update of signal weights based on realized outcomes
    ├── Aggregation:   Weighted combination of surviving signals → final decision
    └── Execution:     Act on the decision (e.g., trade)
```

### Why Online Learning

Batch ML assumptions do not hold:

| Batch assumption       | Reality                              |
|------------------------|--------------------------------------|
| Fixed feature set      | Signals appear and disappear         |
| Bulk training data     | Data arrives asynchronously           |
| Periodic retraining    | Retraining on every signal change is infeasible |

Online weight updating solves this naturally.

### Signal Weight Lifecycle

```
1. New signal arrives     → assigned low initial weight (untrusted)
2. Predictions observed   → weight updated based on realized accuracy
3. Signal proves useful   → weight increases over time
4. Signal goes missing    → excluded from aggregation (no crash)
5. Signal returns         → resumes with its last known weight
```

This follows the "prediction with expert advice" framework — each signal is an
"expert" whose influence is proportional to its track record.

### Key Design Properties

- **No single signal dependency**: the system never requires a specific signal to function
- **Graceful degradation**: fewer signals = lower confidence, not failure
- **Incremental adoption**: new signals start contributing immediately at low weight
- **Interpretability**: signal weights show "what is driving the decision right now"

## Open Questions

- [ ] Scheduler implementation: cron-based, APScheduler, or custom event loop?
- [ ] Storage: keep Parquet files, or move to a time series database?
- [ ] API framework: FastAPI, or something lighter?
- [ ] Consumer project naming and repository structure
- [ ] Specific online learning algorithm (EWA, Follow the Regularized Leader, etc.)
- [ ] How to handle collector authentication (API keys) in service mode
- [ ] Monitoring and alerting for collector failures
