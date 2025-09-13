# USD Pricing Plan

This document describes how to add deterministic, point-in-time USD-denominated metrics to the Zilstream indexer with minimal, localized changes.

- Phase 1: Dependency‑free bootstrap using a CSV ZIL/USD series, minute-bucket storage, cached lookups, and module-level USD fields (Uniswap V2 and Uniswap V3).
- Phase 2: Full oracle with on-chain TWAP, stablecoin routing, and real-time ZIL/USD feeds.

## Overview

- Deterministic USD values at event time (block timestamp) for volumes, liquidity, and fees.
- Storage: minute-bucket ZIL→USD series in Postgres.
- Provider: DB-backed lookup with in-memory cache for throughput.
- Integration: compute/store USD fields in module tables (Uniswap V2 and Uniswap V3) to avoid core schema changes in Phase 1.

```mermaid
flowchart LR
  subgraph Data
    CSV[zilliqa_historical_prices.csv]
  end

  subgraph Services
    Loader[CSV Loader (cmd/load_zil_prices_csv)]
    Provider[PriceProvider (internal/prices)]
    BackfillV2[Backfill USD for UniswapV2 (cmd/backfill_usd_uniswapv2)]
    BackfillV3[Backfill USD for UniswapV3 (cmd/backfill_usd_uniswapv3)]
    Indexer[UnifiedSync + EventProcessor]
    HandlersV2[UniswapV2 Handlers]
    HandlersV3[UniswapV3 Handlers]
  end

  subgraph DB
    P[(prices_zil_usd_minute)]
    U2[(uniswap_v2 tables + usd columns)]
    U3[(uniswap_v3 tables + usd columns)]
  end

  CSV --> Loader --> P
  Indexer --> HandlersV2
  Indexer --> HandlersV3
  HandlersV2 --> Provider
  HandlersV3 --> Provider
  Provider --> P
  HandlersV2 --> U2
  HandlersV3 --> U3
  BackfillV2 --> Provider
  BackfillV2 --> U2
  BackfillV3 --> Provider
  BackfillV3 --> U3
```

## Phase 1 – Bootstrap USD

### 1) Database schema & migrations

- Create table `prices_zil_usd_minute` (minute-precision ZIL→USD):

```sql
CREATE TABLE IF NOT EXISTS prices_zil_usd_minute (
  ts          TIMESTAMPTZ   PRIMARY KEY,      -- truncated to minute (UTC)
  price       NUMERIC(18,9) NOT NULL,         -- ZIL → USD
  source      TEXT          NOT NULL,         -- 'bootstrap_csv' | 'manual' | 'provider_x'
  inserted_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
  UNIQUE (ts, source)
);
```

- Note: The module schemas already include USD fields we will populate:
  - Uniswap V2: pairs.reserve_usd, pairs.volume_usd and swaps/mints/burns.amount_usd as defined in [`005_module_system.sql`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/005_module_system.sql#L64-L87) and [`005_module_system.sql`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/005_module_system.sql#L98-L121).
  - Uniswap V3: pools.volume_usd, pools.fees_usd and swaps/mints/burns/collects.amount_usd as defined in [`007_uniswap_v3.sql`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/007_uniswap_v3.sql#L16-L34) and [`007_uniswap_v3.sql`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/007_uniswap_v3.sql#L40-L123).

Only the `prices_zil_usd_minute` table is new for Phase 1.

### 2) CSV bootstrap loader

- New CLI: `cmd/load_zil_prices_csv`.
- Input: `data/zilliqa_historical_prices.csv` (semicolon-separated, decimal comma, epoch-ms times, daily OHLCV).
- Use `priceClose` per day; upsample to minute buckets with forward-fill (use previous close for each minute in the day).
- Insert in batches (e.g., 10k) using bulk writer or batched inserts.
- Set `source = 'bootstrap_csv'`.
- Idempotent: `ON CONFLICT (ts, source) DO NOTHING`.

### 3) Price provider component

- Package: `internal/prices`.
- Interface:

```go
type Provider interface {
    // Returns the ZIL→USD price for the minute at or prior to ts.
    PriceZILUSD(ctx context.Context, ts time.Time) (decimal.Decimal, bool)
}
```

- Implementation: Postgres provider with prepared statement:
  - `SELECT price FROM prices_zil_usd_minute WHERE ts <= $1 ORDER BY ts DESC LIMIT 1`.
  - In-memory LRU/time-bucket cache (~10k minutes ≈ 1 week) to avoid DB thrash during backfills.
  - Gap handling: nearest prior price; optional max lookback threshold returns `ok=false`.

### 4) Token USD computation in Uniswap V2 and V3

- Uniswap V2 (existing tables):
  - Swaps: compute `uniswap_v2_swaps.amount_usd` using pair price at event time; update `uniswap_v2_pairs.volume_usd` aggregates. See definitions in [`005_module_system.sql`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/005_module_system.sql#L98-L121) and [`005_module_system.sql`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/005_module_system.sql#L64-L87).
  - Mints/Burns: compute `amount_usd` from token amounts at event price.
  - Reserve USD: update `uniswap_v2_pairs.reserve_usd` on Sync using current reserves × price(s).
  - Pricing path in Phase 1: token→ZIL via pair reserves where applicable, then × ZIL→USD; handle direct ZIL pairs immediately.

- Uniswap V3 (existing tables):
  - Swaps: compute `uniswap_v3_swaps.amount_usd` using pool mid-price (from `sqrt_price_x96`) and token USDs at event time. See definitions in [`007_uniswap_v3.sql`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/007_uniswap_v3.sql#L40-L57).
    - If only ZIL has USD price (Phase 1), handle pools where token0 or token1 is ZIL by valuing the non-ZIL leg via the pool price × ZIL→USD.
    - If both legs have USD prices (e.g., one is a stablecoin), compute both and use the average or either leg (they should be equal within fee slippage).
  - Mints/Burns/Collects: compute `amount_usd` for [`mints`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/007_uniswap_v3.sql#L63-L79), [`burns`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/007_uniswap_v3.sql#L85-L101), and [`collects`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/007_uniswap_v3.sql#L107-L123) by valuing amount0/amount1 at event price.
  - Pool aggregates: increment `uniswap_v3_pools.volume_usd` and `fees_usd` based on swap notional and fee tier. See [`pools`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/007_uniswap_v3.sql#L16-L34).
  - Pricing inputs in Phase 1: use per-event `sqrt_price_x96` (mid-price) and ZIL→USD; defer TWAP via observations to Phase 2.

- Provider injection via ModuleRegistry: `uniswapv2.New(priceProvider)` and `uniswapv3.New(priceProvider)` (when module code is wired).

- Safeguards (apply to both V2 and V3):
  - Minimum liquidity thresholds before computing USD (configurable).
  - Optional max lookback for ZIL→USD price to avoid stale valuations.

### 5) Backfill jobs

- `cmd/backfill_usd_uniswapv2`:
  - Scan V2 rows with NULL `amount_usd` or pairs with stale `reserve_usd`.
  - Batch (e.g., 5k), compute via provider, upsert; idempotent and resumable.

- `cmd/backfill_usd_uniswapv3`:
  - Scan V3 `swaps`/`mints`/`burns`/`collects` with NULL `amount_usd`.
  - Batch, compute via provider and pool prices, upsert; update pool aggregates (`volume_usd`, `fees_usd`).

- Progress markers stored in `module_state` metadata or a small `backfill_state` table.

### 6) Config

- Add options with sensible defaults (via Viper):
  - `PRICE_CSV_PATH`: default `data/zilliqa_historical_prices.csv`.
  - `PRICE_CACHE_MINUTES`: default 10080 (7 days).
  - `PRICE_MAX_LOOKBACK`: optional duration (e.g., `72h`).
  - `MIN_LIQUIDITY_USD`: threshold to compute USD values (e.g., $5k).

### 7) Performance & caching

- Targets: sustain 1k+ events/sec during backfills.
- Price lookups are O(1) with high cache hit ratios; DB queries only on cold minutes.
- Consider prefetching consecutive minutes during sequential backfills.

### 8) Testing

- `internal/prices/provider_test.go`: minute lookup, missing minute, max lookback.
- `cmd/load_zil_prices_csv`: decimal comma parsing; upsample correctness (boundaries, UTC minutes).
- `internal/modules/uniswapv2/*_test.go`: swap USD calculation with synthetic price series.
- `internal/modules/uniswapv3/*_test.go`: swap/mint/burn USD with synthetic pool prices (`sqrt_price_x96`) and ZIL→USD series.

### 9) Acceptance criteria

- Migrations apply on clean and existing DBs (only the new `prices_zil_usd_minute` table is added in Phase 1).
- Loader imports ≥ 365×1440 minute rows (or full CSV range) without errors.
- `SELECT price FROM prices_zil_usd_minute ORDER BY ts DESC LIMIT 1;` returns expected value.
- New Uniswap V2 and V3 swaps indexed post-change have non-NULL `amount_usd` when pricing is available (ZIL pairs at minimum).
- Backfills populate historical USD fields for V2 and V3; reruns are NOOP.

## Phase 2 – Full oracle

### A) Token USD routing

- Prefer direct stablecoin pairs (e.g., USDT/USDC) when available.
- Otherwise compute token→ZIL via Uniswap V2/V3 TWAP across N blocks/minutes (use V3 observations), then multiply by ZIL→USD.
- Safeguards: minimum liquidity thresholds, TWAP windows, outlier rejection.

### B) Extended schema

- `prices_token_usd_minute`:

```sql
CREATE TABLE IF NOT EXISTS prices_token_usd_minute (
  token_address BYTEA       NOT NULL,
  ts            TIMESTAMPTZ NOT NULL,
  price         NUMERIC(38,18) NOT NULL,
  source        TEXT        NOT NULL,
  inserted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (token_address, ts)
);
```

### C) Real-time ZIL/USD feed

- Small poller (every minute) to a public API writes into `prices_zil_usd_minute` with `source='coingecko'`.
- Provider prefers the freshest source; falls back to CSV-derived rows.

### D) Gas/tx-level USD

- Add `tx_fee_usd` (or a derived `tx_usd` table) computed during block finalize.

### E) Observability

- Prometheus metrics: price freshness, cache hit ratio, backfill progress.

## Trade-offs & risks

- Localized module changes in Phase 1 avoid wide schema refactors; core tables remain untouched until we add tx_fee_usd.
- CSV-only bootstrap is deterministic and reproducible; real-time feeds are pluggable later.
- Minute buckets are an efficient compromise for 1-second blocks.
- Risks: sparse data days covered by forward-fill; mitigate manipulation in Phase 2 via TWAP and liquidity floors.

## Implementation checklist (Phase 1)

- [ ] Migration: `prices_zil_usd_minute`.
- [ ] `cmd/load_zil_prices_csv`: parse, upsample, bulk insert.
- [ ] `internal/prices`: Provider interface, Postgres provider, LRU cache.
- [ ] Wire Provider into Uniswap V2 and V3 handlers; compute/store USD for events and aggregates.
- [ ] `cmd/backfill_usd_uniswapv2` and `cmd/backfill_usd_uniswapv3`: idempotent, batched backfills.
- [ ] Config flags and unit tests.

## References

- Source CSV: `data/zilliqa_historical_prices.csv`
- Key code areas: `internal/sync/`, `internal/processor/`, `internal/modules/uniswapv2/`, `internal/modules/uniswapv3/`, `internal/database/`
- Existing schema with USD fields: [`005_module_system.sql`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/005_module_system.sql#L64-L121), [`007_uniswap_v3.sql`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/007_uniswap_v3.sql#L16-L123), unified view [`008_all_pools_view.sql`](file:///Users/melvin/Developer/zilstream-indexer/internal/database/migrations/008_all_pools_view.sql#L1-L41)
