# signal-noise: Design Document

> This document captures architectural decisions from a design discussion (2026-02-23).
> It defines the future direction of signal-noise and its relationship with consumer projects.

## Core Principle

**signal-noise has no purpose.** It collects and delivers time series data without
any assumption about what the data will be used for. Prediction, evaluation,
transformation — all of these belong to the consumer.

> "even noise is worth collecting -- the signal hides within"

## Collection Spectrum

Data collection is not binary. Between "fully available via free API" and
"physically impossible to measure" lies a gradient. signal-noise's scope
spans this entire spectrum — including pushing things from the unobservable
side toward the observable.

```
 Easy                                                            Hard
──────────────────────────────────────────────────────────────────────
 L1        L2         L3         L4         L5          L6        L7
 Free API  Auth API   Scraping   Proxy      Active      Physical  Not yet
                                            probing     sensors   defined
```

### L1: Free API consumption (current majority)

No authentication. JSON response. Immediate implementation.

Examples: Open-Meteo, CoinGecko, USGS earthquake, ISS position, NOAA alerts

### L2: Authenticated API consumption

Free registration for API key. Same integration pattern as L1.

Examples: FRED, BLS, EIA, BEA

### L3: Scraping / parsing unstructured sources

Data is publicly available but not via a structured API. Requires HTML
parsing, PDF extraction, or CSV download + transformation.

Examples: government statistical releases, court filings, regulatory bulletins

### L4: Proxy derivation

The phenomenon of interest cannot be directly measured, but correlated
observables exist. The proxy is explicitly labeled as such — not presented
as the real thing.

Examples: Wikipedia pageviews as attention proxy, shipping ETF prices as
maritime activity proxy, npm downloads as tech adoption proxy

### L5: Active probing

signal-noise's own servers become sensors. No external API is consumed —
the measurement originates from our infrastructure.

Examples:
- Network latency (ping/traceroute to global endpoints)
- DNS resolution time to major services
- HTTP response time of public APIs (meta-observability)
- TLS certificate health monitoring

### L6: Physical sensors

Deploy hardware to measure the physical world where no existing data
source covers. Even a single $20 sensor at one location creates a time
series that previously did not exist.

Examples:
- Temperature / humidity / barometric pressure (BME280 sensor)
- Air quality / PM2.5 (SDS011 or PMS5003 sensor)
- Ambient noise level (microphone + dB computation)
- RF spectrum occupancy (SDR dongle)
- Light level / UV index (photodiode)

### L7: Not yet defined

Concepts that matter but lack any known measurement method. These are
documented as open problems, not dismissed as out of scope.

Examples: institutional trust, social cohesion, regulatory intent,
innovation velocity

### Guiding principles

- **Every level is in scope.** signal-noise is not just an API aggregator.
  It is an observatory that actively expands the boundary of what is
  observable.
- **Label the level.** Each collector should declare its collection level
  so consumers know what they are getting (direct measurement vs proxy
  vs active probe).
- **Prefer lower levels.** L1 data is cheaper and more reliable. Move to
  higher levels only when lower levels cannot cover the phenomenon.
- **Document what is missing.** Even if we cannot collect it today,
  recording "this is not observable yet" has value — it defines the
  frontier.

### Unreached but reachable sources (as of 2026-03-01)

Verified free APIs not yet integrated:

| Source | Level | Endpoint | Data | Auth |
|--------|-------|----------|------|------|
| Alpha Vantage | L2 | `alphavantage.co/query` | Equities, FX, crypto | Free key (25/day limit) |

Previously listed sources now implemented:

| Source | Level | Collector file | Collectors |
|--------|-------|----------------|:---:|
| Finnhub | L2 | `finnhub_generic.py` | 42 |
| BEA | L2 | `bea_generic.py` | 12 |
| NOAA CO-OPS | L1 | `noaa_coops.py` | 8 |
| UK Carbon Intensity | L1 | `uk_carbon_intensity.py` | 2 |
| Eurostat | L1 | `eurostat_generic.py` | 16 |
| Bank of Canada | L1 | `boc_generic.py` | 10 |
| openFDA | L1 | `openfda.py` | 3 |
| CDC Socrata | L1 | `cdc_flu.py`, `cdc_wastewater.py` | 5 |
| USGS Water Services | L1 | `usgs_water.py` | 8 |
| Bank of England | L1 | `boe_generic.py` | 5 |
| BIS Statistics | L3 | `bis_property.py` | 80 |

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

- **Raw time series only**: ~1,256 collector outputs
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
- **Data volume**: ~1,238 series × ~2,000 rows ≈ 38 MB — trivially small

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

## Potential Use Cases

signal-noise has no purpose — but its data has many potential uses. This section
documents every use case we can imagine, without filtering by feasibility or
priority. The list serves as a creative inventory, not a roadmap.

### Trading & Finance

1. **Quantitative trading signals** — alpha generation from cross-domain time series (current use via alpha-os)
2. **Recession early warning** — composite stress indicators from labor, credit, freight, volatility
3. **Inflation nowcasting** — price pressures via CPI components, shipping costs, commodities, wages
4. **Financial stress index** — VIX, credit spreads, liquidity proxies, shipping, news intensity
5. **Cross-asset correlation regime map** — detect when diversification fails or returns
6. **Risk parity / allocation context** — macro + volatility + liquidity signals for portfolio posture
7. **Central bank policy regime detector** — ECB/BOJ/PBOC signals + yields, FX, risk assets
8. **Currency crisis early warning** — FX, reserves proxies, inflation, news stress indicators
9. **Sovereign risk monitor** — bond yields + macro deterioration + geopolitics intensity
10. **Emerging market fragility index** — commodity terms-of-trade + FX + shipping + news
11. **Commodity supercycle detection** — multi-commodity trends + shipping + industrial production
12. **Sector rotation explainer** — macro regime + commodity + rates + geopolitics drivers
13. **Earnings surprise context** — leading indicators for specific industries
14. **Hedge fund research backbone** — fast context layer for discretionary + systematic teams
15. **Family office briefing** — weekly "world context" memo from multi-domain indicators

### Crypto

16. **Crypto market health monitor** — hashrate, mempool, DeFi liquidations, stablecoin stress
17. **On-chain / off-chain coupling study** — crypto network activity vs macro/tech adoption proxies
18. **Crypto protocol risk dashboard** — liquidation cascades + mempool + macro stress alignment
19. **DeFi systemic risk tracking** — cross-protocol contagion via shared collateral stress

### Geopolitics & Security

20. **Geopolitical risk monitor** — GDELT topic spikes + market/commodity reactions
21. **Trade war / sanctions impact tracker** — GDELT + shipping + FX + sector equities
22. **Social unrest risk proxy** — news intensity + inflation + unemployment + sentiment
23. **Election cycle impact map** — political news volume + market/commodity responses
24. **Misinformation early detection** — topic surges + Wikipedia edit wars + social chatter
25. **Compliance / AML context** — geopolitical events + flow proxies + market anomalies

### Supply Chain & Trade

26. **Supply chain disruption radar** — port congestion, freight indices, AIS traffic anomalies
27. **Maritime chokepoint alerting** — congestion around key straits/ports + freight spikes
28. **Corporate supply risk scoring** — procurement risk from freight/ports/geopolitics
29. **Shipping company capacity planning** — forward-looking congestion and demand proxies
30. **Retail demand proxy** — macro + mobility + sentiment + freight for inventory planning

### Climate & Natural Phenomena

31. **Climate / ENSO impact tracker** — ENSO + weather extremes + commodity/insurance proxies
32. **Space weather operations alerting** — geomagnetic storms vs comms/logistics/infra proxies
33. **Disaster impact estimation** — earthquakes/storms → trade, mobility, prices, news volume
34. **Food security watch** — weather/ENSO + grain prices + freight + geopolitics
35. **Energy transition watch** — renewables/commodities/macros reflecting electrification trends

### Health & Society

36. **Pandemic / health surveillance** — WHO signals + mobility proxies + sentiment shifts
37. **Consumer stress pulse** — inflation proxies + TSA throughput + sentiment/social activity
38. **Cost-of-living tracker** — Numbeo + macro releases + wage proxies over time
39. **Labor market dynamics explorer** — BLS series + tech hiring proxies + market rotations
40. **Housing cycle monitor** — Census/housing starts + rates + materials + mobility
41. **Tourism recovery tracker** — TSA throughput + FX + macro + health alerts
42. **Migration / mobility proxy** — TSA + macro data + geopolitical news shifts

### Technology & Developer Ecosystem

43. **Tech adoption tempo index** — npm/PyPI/crates downloads + GitHub activity + HN attention
44. **Open-source ecosystem health** — maintainer activity, release cadence, dependency churn
45. **AI / ML hype cycle meter** — HN/GDELT/GitHub topic bursts vs market proxies
46. **Developer productivity / morale proxy** — Stack Overflow/HN trends vs macro stress
47. **Innovation diffusion study** — new libraries/tools → hiring → valuations → productivity
48. **Vulnerability risk proxy** — sudden package download spikes + exploit/news correlation

### Anomaly Detection & Pattern Discovery

49. **Anomaly-of-the-day feed** — daily "weirdness" report across domains
50. **Regime change detector** — identify phase transitions in the civilization state vector
51. **Unexpected coupling discovery** — detect when unrelated domains move together
52. **Broken coupling alerting** — detect when correlated series suddenly decouple
53. **Lead-lag reversal detection** — find when A→B causality flips to B→A
54. **Narrative dominance detector** — news volume explodes but reality indicators stay flat
55. **Measurement anomaly flagging** — detect API changes, data revisions, collection artifacts

### Curiosity & Understanding

56. **Question generation engine** — surface surprising couplings + suggested investigations
57. **"World debugger" workflow** — symptoms → suspects → disconfirmers → follow-up data
58. **Civilization "build status" page** — like CI for the world (green/yellow/red subsystems)
59. **Causal discovery playground** — search for stable causal graphs across regimes
60. **News-market reflexivity lab** — how narratives propagate into prices and back
61. **Narrative-to-policy pipeline analysis** — topic spikes → central bank language → markets

### Academic & Research

62. **Econometrics sandbox** — stress-test models across heterogeneous time series
63. **Social science research dataset** — unified multi-domain data for hypothesis testing
64. **Forecasting competition benchmark** — host prediction challenges with real-world data
65. **Academic replication platform** — stable storage for historical daily series snapshots
66. **Policy evaluation tracker** — measure effects of interventions across multiple indicators
67. **Complex systems teaching tool** — live data labs for macro/network/chaos courses

### Journalism & Communication

68. **Quant journalism assistant** — story leads backed by multi-source time series shifts
69. **Investigative reporting triage** — detect coordinated behavior or hidden shocks
70. **Educational newsletter** — daily/weekly "3 strange charts" with explanations
71. **Fraud / manipulation spotting** — odd co-movements across news/social/markets

### Humanitarian & Public Good

72. **NGO resource allocation aid** — prioritize regions via stress signals (health, food, conflict)
73. **Philanthropy timing signal** — when/where stress rises to act faster
74. **Insurance risk context** — weather extremes + economic stress + supply-chain risk
75. **Commodity producer ops planning** — align output/hedging with macro + logistics

### Creative & Experimental

76. **"Reality compression" art** — visualizations/sonifications of the world state vector
77. **Generative music engine** — map macro/geo/weather/crypto rhythms to sound
78. **Data-driven fiction prompts** — world anomalies become plot seeds
79. **Interactive museum exhibit** — "living planet" dashboard with daily narratives
80. **Game world simulator input** — drive in-game economy/weather/news from real data
81. **Meditation / reflection tool** — daily "world pulse" to contextualize personal concerns

### Infrastructure & Platform

82. **Public data API** — serve unified time series to other projects and developers
83. **Data quality monitoring platform** — track API reliability across 1052 sources
84. **Historical snapshot archive** — immutable daily snapshots for reproducibility
85. **Webhook / event streaming** — push notifications when specific thresholds are crossed
86. **Multi-tenant data service** — different consumers subscribe to different signal subsets
87. **Collector marketplace** — community-contributed collectors with standardized interface

## Open Questions

- [x] Scheduler implementation: cron-based, APScheduler, or custom event loop?
  → asyncio self-managed loop (implemented in `scheduler/loop.py`)
- [x] Storage: keep Parquet files, or move to a time series database?
  → SQLite with WAL mode (implemented in `store/sqlite_store.py`)
- [x] API framework: FastAPI, or something lighter?
  → FastAPI (implemented in `api/app.py`)
- [ ] Consumer project naming and repository structure
- [ ] Specific online learning algorithm (EWA, Follow the Regularized Leader, etc.)
- [ ] How to handle collector authentication (API keys) in service mode
- [ ] Monitoring and alerting for collector failures
- [ ] Domain taxonomy: current 10-domain classification has mixed axes
  (object vs method vs actor). Revisit when collection spectrum expands
  beyond L1/L2 — taxonomy should emerge from the data, not precede it.
- [ ] CollectorMeta: add `collection_level` field (L1–L7) so consumers
  can distinguish direct measurements from proxies
- [ ] L5 active probing: design collector base class for self-originated
  measurements (no external API, measurement from own infrastructure)
- [ ] L6 physical sensors: evaluate Raspberry Pi + sensor HAT as first
  physical observation point
