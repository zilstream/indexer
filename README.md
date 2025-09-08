# Zilstream Indexer

High-performance Zilliqa EVM indexer with unified sync, event-log storage, and a pluggable module system (Uniswap V2 module included). Designed to keep up with ~1s block times while handling Zilliqa pre‑EVM transactions gracefully.

## Highlights

- Unified sync with adaptive batching (near-head 1-by-1, deep backfill in large batches)
- Batch RPC for blocks and receipts with client-side rate limiting
- Pre‑EVM Zilliqa transaction compatibility (fallbacks and minimal receipts)
- Event log extraction into `event_logs` for downstream processing
- Pluggable module system driven by YAML manifests (`manifests/`)
- Health and status endpoints exposed over HTTP

## Requirements

- Go 1.25+
- PostgreSQL 15+
- Zilliqa EVM RPC endpoint

## Quick start

1. Start PostgreSQL (fresh DB will auto-run migrations from this repo):

```bash
docker-compose up -d postgres
```

2. Configure the indexer:

Update `config.yaml` (RPC, DB credentials, batch sizes, etc.). Example:

```yaml
server:
  port: 8080
  metrics_port: 9090

chain:
  name: "zilliqa"
  chain_id: 32769
  rpc_endpoint: "http://localhost:4202"
  block_time: 1s
  start_block: 0

database:
  host: "localhost"
  port: 5432
  name: "zilstream"
  user: "postgres"
  password: "postgres"
  ssl_mode: "disable"
  max_connections: 10

processor:
  batch_size: 100
  workers: 5
  requests_per_second: 50
  max_retries: 3
  retry_delay: 1s

logging:
  level: "info"
  format: "json"
```

3. Build and run:

```bash
make build
./bin/indexer --config=config.yaml
```

Or run directly:

```bash
make run
```

Notes on migrations:

- With Docker Compose, migrations are mounted to `/docker-entrypoint-initdb.d` and apply automatically on first start.
- Running Postgres locally? Either use `make db-create && make migrate-up` (adjust user/host in the Makefile) or apply SQL files with `psql` from `internal/database/migrations/`.

## How it works

### Unified sync

- Adaptive batches based on gap size (see `internal/sync/unified_sync.go`).
- Uses `internal/rpc/BatchClient` to batch `eth_getBlockByNumber` and `eth_getTransactionReceipt` calls with `requests_per_second` rate limiting.
- Writes small batches atomically with `AtomicBlockWriter`; switches to `BulkWriter` for larger batches (>= 50 blocks).
- Extracts and stores all event logs in `event_logs` for downstream consumers.

### Zilliqa pre‑EVM support

- Falls back to raw RPC for problematic blocks and constructs minimal receipts when necessary (see `internal/rpc/client.go` and `batch_client.go`).

### Health and status endpoints

Once running, the indexer serves:

- `GET /health` → simple health check
- `GET /status` → `{ lastIndexedBlock, latestBlock, blocksIndexing, synced, chainId }`

Endpoints are served on `server.metrics_port` (default `:9090`).

## Module system

Modules are defined by manifests in `manifests/` and registered at startup. Currently the Uniswap V2 module is implemented and loaded via its manifest:

- Manifest: `manifests/uniswap-v2.yaml`
- Module: `internal/modules/uniswapv2`
- Registry: `internal/modules/core/registry.go`

At startup, the indexer loads manifests from `manifests/`, initializes the Uniswap V2 module for each, and routes relevant `event_logs` to the module handlers.

### Backfill a module

You can backfill a module over a historical block range using the backfill CLI:

```bash
go build -o bin/backfill ./cmd/backfill
./bin/backfill --module uniswap-v2 --from 3250780 --to 3260000 --config=config.yaml
```

- If `--from`/`--to` are omitted, it defaults to the min/max from `event_logs`.
- Backfill reads from `event_logs` and replays events through the module.

## Configuration reference

- `server.port` / `server.metrics_port`
- `chain.name`, `chain.chain_id`, `chain.rpc_endpoint`, `chain.block_time`, `chain.start_block`
- `database.host`, `database.port`, `database.name`, `database.user`, `database.password`, `database.ssl_mode`, `database.max_connections`
- `processor.batch_size`, `processor.workers`, `processor.requests_per_second`, `processor.max_retries`, `processor.retry_delay`
- `logging.level` (`debug|info|warn|error`), `logging.format` (`json|console`)

Environment variables are supported via prefix `INDEXER_` (e.g. `INDEXER_DATABASE_PASSWORD`).

## Database model (core tables)

- `blocks` — block header and meta (`number`, `hash`, `parent_hash`, `timestamp`, gas fields)
- `transactions` — tx details + Zilliqa-specific fields (`transaction_type`, etc.)
- `event_logs` — raw EVM logs (topics as JSONB, data as hex)
- `indexer_state` — last indexed block per chain
- `module_state` — per-module progress/status

Uniswap V2 entities (subset): `uniswap_v2_factory`, `uniswap_v2_pairs`, `uniswap_v2_swaps`, `uniswap_v2_mints`, `uniswap_v2_burns`, `uniswap_v2_syncs`, plus universal `tokens`.

## Development

Common Make targets:

```bash
make help            # List commands
make build           # Build indexer
make run             # Run with config.yaml
make test            # Run tests
make db-create       # Create DB (adjust user/host in Makefile if needed)
make migrate-up      # Apply SQL migrations
make db-reset        # Drop + recreate + migrate
docker-compose up -d # Start Postgres for local dev
```

## Troubleshooting

- RPC errors or slow responses: lower `processor.requests_per_second` and/or `processor.batch_size`.
- Connection refused to Postgres: ensure Docker container is healthy (`docker ps`) and config points to `localhost:5432`.
- No events processed by module: confirm logs exist in `event_logs`, the manifest address/signatures are correct, and the module registered (see startup logs and `/status`).
- Migrations didn’t apply: for fresh Docker volumes they apply automatically; otherwise run `make migrate-up` or use `psql` to apply files in `internal/database/migrations/`.

## License

MIT
