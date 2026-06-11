# circuit-indexer

Consumes a Geyser event stream from the Solana blockchain, parses Raydium, Orca, and PumpSwap pool state, and writes real-time price data to Redis with OHLCV candles. Part of the Circuit data infrastructure stack.

## What it does

- **Parses** Raydium AMM v4, Raydium CLMM, Orca Whirlpool, and PumpSwap pool account updates into structured price/pool records
- **Tracks swap transactions** — extracts buy/sell direction and SOL volume from token balance deltas, fires into OHLCV aggregator with accurate b/s counts
- **Tracks** token mint metadata (decimals, supply, authorities)
- **Writes hot data** to Redis with short TTLs for low-latency reads by circuit-price-feed
- **Writes OHLCV candles** to Redis ring buffers (1m/5m/1h/1d) with buy/sell counts — consumed by the scan route for on-chain dip discovery
- **Three input modes**: file (from circuit-geyser), stdin (piped from test-validator), or gRPC (managed Geyser endpoint)

## How it fits in the Circuit stack

```
[circuit-geyser .so]          [Managed Geyser gRPC]
     │  Redis Stream                    │  Triton / Helius / QuickNode
     └──────────────┬───────────────────┘
                    ▼
            circuit-indexer
            ├─ Redis: circuit:price-sol:{mint}         (TTL 120s)
            ├─ Redis: circuit:pool:{account}           (TTL 60s)
            ├─ Redis: circuit:pool-by-mint:{mint}      (TTL 120s)
            ├─ Redis: circuit:mint:{mint}              (no TTL)
            ├─ Redis: circuit:trending                 (ZSET, delta swap volume)
            ├─ Redis: circuit:candles:{window}:{mint}  (ring buffer, 1m/5m/1h/1d)
            └─ Redis: circuit:ph:{mint}                (price history ticks, TTL 24h)
                    ▼
            circuit-price-feed (serves /price, /candles, /losers, /trending)
```

## Prerequisites

| Requirement | Notes |
|---|---|
| Node.js ≥ 18 | `node --version` |
| Redis ≥ 6.2 | Optional — indexer runs in no-op mode if unavailable |
| PostgreSQL ≥ 14 | Optional — candle/pool history only |
| A Geyser event source | circuit-geyser, a managed gRPC endpoint, or a test file |

Redis and Postgres are `optionalDependencies` — the indexer starts and logs a warning if they are not installed.

## Installation

```bash
git clone https://github.com/Circuit-LLM/circuit-indexer
cd circuit-indexer
npm install
```

To enable Redis and Postgres support:

```bash
npm install ioredis pg
```

To enable gRPC (Triton/Helius/QuickNode Geyser):

```bash
npm install @triton-one/yellowstone-grpc
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection string |
| `DATABASE_URL` | `postgresql://localhost/circuit_index` | Postgres connection string |
| `GEYSER_FILE` | `/tmp/circuit-geyser.jsonl` | Input file path (file consumer) |
| `GEYSER_ENDPOINT` | — | gRPC endpoint URL (grpc consumer) |
| `GEYSER_TOKEN` | — | gRPC access token (grpc consumer) |

## Running

### File consumer (default)

Reads JSON lines from a file written by circuit-geyser or test-validator:

```bash
node indexer.js
# or specify a path explicitly:
node indexer.js --consumer=file --file=/tmp/circuit-geyser.jsonl
```

### stdin consumer

Pipe test-validator output directly:

```bash
solana-test-validator \
  --geyser-plugin-config /path/to/circuit-geyser/config/local.json \
  2>/dev/null | node indexer.js --consumer=stdin
```

### gRPC consumer

Connect to a managed Geyser endpoint (Helius, Triton One, QuickNode):

```bash
GEYSER_ENDPOINT=https://your-endpoint.rpc.triton.one:10000 \
GEYSER_TOKEN=your-token \
node indexer.js --consumer=grpc
```

This is the recommended production setup when running circuit-node without a local validator. Managed endpoints provide the same data stream as a local Geyser plugin.

## Redis data layout

| Key pattern | Type | TTL | Contents |
|---|---|---|---|
| `circuit:price:{mint}` | STRING | 30s | `{ price, type, ts }` |
| `circuit:pool:{account}` | STRING | 60s | Full pool state |
| `circuit:mint:{mint}` | STRING | none | Mint metadata (decimals, supply, authorities) |
| `circuit:trending` | ZSET | rolling | score = 5m volume USD, member = mint address |

## Postgres schema

Three tables are created automatically on first run:

**`pools`** — pool state snapshots (Raydium, Orca)
```sql
id, pubkey, program, type, mint0, mint1, price,
reserve0, reserve1, fee, slot, ts
```

**`tokens`** — token mint records
```sql
mint, decimals, supply, mint_authority, freeze_authority,
is_token2022, slot, ts
```

**`candles`** — OHLCV candles (1m aggregation, written at interval close)
```sql
mint, interval, open_ts, close_ts,
open, high, low, close, volume, trades
```

## Supported DEXes

| DEX | Program | Parser |
|---|---|---|
| Raydium AMM v4 | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` | `parsers/raydium.js` |
| Raydium CLMM | `CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK` | `parsers/raydium.js` |
| Orca Whirlpools | `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc` | `parsers/orca.js` |
| PumpSwap | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` | `parsers/pumpswap.js` |

Additional DEX parsers can be added by implementing the `processAccountEvent(event)` interface in `parsers/`.

## Stats logging

The indexer logs aggregate stats every 30 seconds:

```
circuit-indexer | Indexer stats { uptime: 5.0m, events: 42000, accounts: 38000,
  txns: 4000, slots: 450, pools: 1200, tokens: 80, candles: 24, errors: 0, eps: 140.0 }
```

## Graceful shutdown

The indexer handles `SIGTERM` and `SIGINT`: flushes pending OHLCV candles, disconnects Redis and Postgres cleanly.

## Systemd deployment

```ini
[Unit]
Description=circuit-indexer
After=network-online.target redis.service postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=watchtower
WorkingDirectory=/home/watchtower/circuit-indexer
ExecStart=/usr/bin/node indexer.js --consumer=grpc
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=circuit-indexer
Environment=NODE_ENV=production
Environment=REDIS_URL=redis://127.0.0.1:6379
Environment=DATABASE_URL=postgresql://localhost/circuit_index
Environment=GEYSER_ENDPOINT=https://your-endpoint.rpc.triton.one:10000
Environment=GEYSER_TOKEN=your-token

[Install]
WantedBy=default.target
```

## License

MIT — see [LICENSE](LICENSE)

## Part of the Circuit stack

- [circuit-geyser](https://github.com/Circuit-LLM/circuit-geyser) — Agave validator Geyser plugin
- **circuit-indexer** — this repo, stream consumer and data writer
- [circuit-node](https://github.com/Circuit-LLM/circuit-node) — RPC aggregator + data API
- [circuit-agent](https://github.com/Circuit-LLM/circuit-agent) — autonomous trading agent
- [circuitllm.xyz](https://circuitllm.xyz) — website and data terminal
