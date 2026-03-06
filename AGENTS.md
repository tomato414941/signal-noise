# signal-noise

Collect worldwide time series and deliver via REST API + WebSocket.

> "even noise is worth collecting -- the signal hides within"

## Terminology

| Term | Definition | Example |
|------|-----------|---------|
| **Provider** | External API / service | Yahoo Finance, FRED, CoinGecko |
| **Collector** | Class that fetches one time series | `BtcOhlcvCollector`, `FearGreedCollector` |
| **CollectorMeta** | Collector metadata (name, domain, category, signal_type, collection_level, interval) | `CollectorMeta(name="fear_greed", ...)` |
| **Signal** | A raw time series delivered via API | `fear_greed`, `btc_ohlcv` |
| **Domain** | Stable top-level grouping (6 types) | markets, economy, environment |
| **Category** | Concrete classification (~61 types) | equity, weather, labor |

### Domain List

| Domain | Description | Categories |
|--------|-----------|------------|
| markets | Equities, FX, bonds, commodities, crypto, derivatives | equity, crypto, crypto_derivatives, forex, rates, commodity, microstructure, regulatory |
| economy | GDP, employment, inflation, housing, agriculture, energy, inequality | economic, labor, inflation, trade, fiscal, real_estate, food_price, agriculture, energy, inequality, tourism, manufacturing, retail |
| environment | Weather, climate, ocean, geology, space weather, biodiversity, noise | weather, climate, marine, air_quality, hydrology, satellite, space_weather, seismic, celestial, wildlife, biodiversity, noise, cryosphere |
| technology | Software ecosystems, internet, logistics, aviation, R&D | developer, academic, patents, logistics, aviation, internet, space, transportation, safety |
| sentiment | Opinions, attention, expectations, prediction markets, gaming | sentiment, attention, prediction_market, temporal, gaming |
| society | Health, mortality, conflict, demographics, governance, legislation | epidemiology, public_health, excess_deaths, cause_of_death, armed_conflict, displacement, city_stats, crime, governance, demographics, education, legislation, food_security |

### Scale

- **Domains**: 6
- **Categories**: 61
- **Collectors**: ~1.7k time series

## Architecture

```
Polling Collectors ──→ SQLite Store ──→ REST API ──→ Consumer
(per-collector          (WAL mode)      (FastAPI)
 frequency)                  ↑
                             │
Streaming Collectors ──→ signals_realtime ──→ EventBus ──→ WebSocket
(Binance WS)           (1-min, 30d retain)   (pub/sub)   (/ws/signals)
```

### Modules

| Module | Role |
|--------|------|
| `collector/base.py` | `BaseCollector` ABC, `CollectorMeta`, taxonomy constants |
| `collector/__init__.py` | Collector registry (`COLLECTORS` dict), `collect_all()` |
| `collector/_auth.py` | Unified API key loading (`load_secret`, `load_secrets`) |
| `collector/_utils.py` | Shared utilities (`build_timeseries_df`) |
| `collector/_cache.py` | `SharedAPICache` — thread-safe TTL cache with stampede prevention |
| `collector/streaming.py` | `StreamingCollector` ABC for WebSocket-based collectors |
| `collector/binance_ws.py` | Binance WS collectors (orderbook, trade flow, liquidation, funding rate) |
| `collector/*.py` | Individual polling collector implementations |
| `store/sqlite_store.py` | `SignalStore` — `signals` + `signals_realtime` tables |
| `store/_health.py` | Pure functions for signal health classification |
| `store/_anomaly.py` | Pure functions for anomaly detection (robust MAD statistics) |
| `store/event_bus.py` | `EventBus` — in-process pub/sub for signal updates |
| `store/migration.py` | Parquet → SQLite migration |
| `analysis/quality.py` | Signal health scoring (completeness, freshness, stability, independence) |
| `analysis/spectrum.py` | SVD spectral redundancy analysis |
| `scheduler/loop.py` | Priority queue scheduler + worker pool + streaming task runner |
| `scheduler/state.py` | `ScheduleQueue` (min-heap), `ScheduleEntry`, `CircuitBreakerState` |
| `api/app.py` | FastAPI REST API + WebSocket endpoint |
| `cli.py` | CLI entrypoint |
| `config.py` | Paths, `CollectorConfig` |
| `client.py` | `SignalClient` — REST + WebSocket subscribe for consumers |

## CLI

```bash
python -m signal_noise collect                # Fetch all collectors (SQLite)
python -m signal_noise collect -s fear_greed  # Fetch specific collector
python -m signal_noise collect -f daily       # Filter by frequency
python -m signal_noise collect --force        # Ignore cache
python -m signal_noise list                   # List collectors with status
python -m signal_noise count                  # Show collector count
python -m signal_noise serve                  # Start scheduler + REST API
python -m signal_noise serve --no-scheduler   # API only
python -m signal_noise analyze spectrum       # SVD spectral analysis
python -m signal_noise analyze quality        # Signal health scoring
python -m signal_noise coverage               # Domain/category coverage matrix
python -m signal_noise rebuild-manifest       # Rebuild collector discovery cache
python -m signal_noise backfill               # Fetch extended historical data
python -m signal_noise rollup-realtime        # Rollup 1-min data to daily + purge old data
```

## REST API

```
GET /health                           # Service health check
GET /health/signals                   # Per-signal health details (stale, failing, never_seen)
GET /health/events                    # EventBus status and subscriber count
GET /signals                          # List all signals (name, domain, category, ...)
GET /signals/{name}                   # Signal metadata
GET /signals/{name}/data?since=...    # Time series data (auto-fallback to realtime for microstructure)
GET /signals/{name}/latest            # Latest value + timestamp
GET /signals/{name}/realtime?since=.. # Realtime (1-min) data for microstructure signals
GET /signals/{name}/anomalies         # Anomaly detection (z-score)
POST /signals/batch                   # Batch query multiple signals
GET /audit                            # Audit log of data changes
WS /ws/signals?names=...             # WebSocket real-time signal events
```

## API Keys (L2 collectors)

L2 collectors require API keys stored in `~/.secrets/`:

| Provider | Env var | Secret file | Collectors |
|----------|---------|-------------|:----------:|
| FRED | `FRED_API_KEY` | `~/.secrets/fred` | 81 |
| EIA | `EIA_API_KEY` | `~/.secrets/eia` | 42 |
| BEA | `BEA_API_KEY` | `~/.secrets/bea` | 12 |
| Finnhub | `FINNHUB_API_KEY` | `~/.secrets/finnhub` | 42 |
| ENTSO-E | `ENTSOE_API_KEY` | `~/.secrets/entsoe` | 4 |
| FBI Crime | `FBI_API_KEY` | `~/.secrets/fbi` | 6 |
| Congress.gov | `CONGRESS_API_KEY` | `~/.secrets/congress` | 5 |
| Twitch | `TWITCH_CLIENT_ID/SECRET` | `~/.secrets/twitch` | 7 |
| Reddit | `REDDIT_CLIENT_ID/SECRET` | `~/.secrets/reddit` | 3 |
| Sonitus | `SONITUS_USER/PASSWORD` | `~/.secrets/sonitus` | 4 |

Secret file format: `export KEY_NAME=value`

All key loading is handled by `collector/_auth.py` (`load_secret` / `load_secrets`).
Pattern: env var → `~/.secrets/{provider}` file → raise RuntimeError with signup URL.

## Adding a New Collector

### Polling collector (BaseCollector)
1. Create `src/signal_noise/collector/<name>.py`
2. Subclass `BaseCollector`, define `meta: CollectorMeta` with correct `domain` and `category`
3. Implement `fetch()` → return `DataFrame` with `[timestamp|date, value]` columns
4. Auto-discovered via `get_<name>_collectors()` factory or direct `BaseCollector` subclass
5. Add tests in `tests/`

### Streaming collector (StreamingCollector)
1. Subclass `StreamingCollector` in `collector/binance_ws.py` (or new file)
2. Set `use_realtime_store = True` for 1-min data → `signals_realtime` table
3. Implement `async stream()` → yield `DataFrame` per aggregation window
4. Multi-signal: include `name` column in DataFrame for per-signal routing

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v                      # 764 tests
ruff check src/ tests/
python -m signal_noise count          # Show collector count
```
