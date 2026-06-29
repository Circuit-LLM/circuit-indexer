<div align="center">

# circuit-indexer

**Consumes a Geyser event stream, parses Raydium, Orca, and PumpSwap pool state and swap transactions, and writes real-time prices and OHLCV candles to Redis. The parsing layer that turns the raw on-chain firehose into the hot data the Circuit stack serves.**

[![Node.js](https://img.shields.io/badge/node-%3E%3D18-brightgreen)](https://nodejs.org)
[![Version](https://img.shields.io/badge/version-0.7.0-blue)](https://github.com/Circuit-LLM/circuit-indexer/releases)
[![Status](https://img.shields.io/badge/status-beta-orange)](https://github.com/Circuit-LLM/circuit-indexer)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

> **Beta software.** circuit-indexer is under active development. Parser layouts track on-chain program changes, so pin a version in production and watch the stats log for a rising `errors` count after any DEX program upgrade.

[Website](https://circuitllm.xyz) · [OPS Terminal](https://circuitllm.xyz/data) · [Telegram](https://t.me/circuitllm) · [X / Twitter](https://x.com/CircuitLLM)

</div>

---

**[What it does](#what-it-does)** · **[How it fits](#how-it-fits-in-the-circuit-stack)** · **[Before you start](#before-you-start)** · **[Quick Start](#quick-start)** · **[Configuration](#configuration)** · **[Running](#running)** · **[Redis layout](#redis-data-layout)** · **[Supported DEXes](#supported-dexes)** · **[Deployment](#deployment)** · **[Docs](#docs)**

---

## What it does

- **Parses** Raydium AMM v4, Raydium CLMM, Orca Whirlpool, and PumpSwap pool account updates into structured price/pool records.
- **Tracks swap transactions** — extracts buy/sell direction and SOL volume from token balance deltas and fires them into the OHLCV aggregator with accurate b/s counts.
- **Tracks token mint metadata** — decimals, supply, and authorities.
- **Writes hot data to Redis** with short TTLs for low-latency reads by circuit-price-feed.
- **Writes OHLCV candles** to Redis ring buffers (1m/5m/1h/1d) with buy/sell counts — consumed by the scan route for on-chain dip discovery.
- **Three input modes** — file (from circuit-geyser), stdin (piped from test-validator), or gRPC (managed Geyser endpoint).

---

## How it fits in the Circuit stack

```
[circuit-geyser .so]          [Managed Geyser gRPC]
     │  Redis Stream                    │  Triton / Helius / QuickNode
     └──────────────┬───────────────────┘
                    ▼
            circuit-indexer
            ├─ Redis: circuit:price:{mint}             (USD price, TTL 30s)
            ├─ Redis: circuit:price-sol:{mint}         (SOL price, TTL 120s)
            ├─ Redis: circuit:pool:{account}           (pool state, TTL 60s)
            ├─ Redis: circuit:pool-by-mint:{mint}      (reverse index, TTL 24h)
            ├─ Redis: circuit:mint:{mint}              (mint metadata, no TTL)
            ├─ Redis: circuit:trending                 (ZSET, accumulated SOL volume)
            ├─ Redis: circuit:candles:{window}:{mint}  (ring buffer, 1m/5m/1h/1d)
            └─ Redis: circuit:ph:{mint}                (price history ticks, TTL 24h)
                    ▼
            circuit-price-feed (serves /price, /candles, /losers, /trending)
```

---

## Before you start

| Requirement | Why | Notes |
|---|---|---|
| **Node.js ≥ 18** | Runtime | `node --version` |
| **Redis ≥ 6.2** | Hot data store | Optional — indexer runs in no-op mode if unavailable |
| **PostgreSQL ≥ 14** | History | Optional — candle/pool history only |
| **A Geyser event source** | Input | circuit-geyser, a managed gRPC endpoint, or a test file |

Redis and Postgres are `optionalDependencies` — the indexer starts and logs a warning if they are not installed.

---

## Quick Start

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

Then start it against your event source (see [Running](#running)):

```bash
node indexer.js                      # file consumer (default), /tmp/circuit-geyser.jsonl
node indexer.js --consumer=grpc      # managed Geyser endpoint (production)
```

---

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection string |
| `DATABASE_URL` | `postgresql://localhost/circuit_index` | Postgres connection string |
| `GEYSER_FILE` | `/tmp/circuit-geyser.jsonl` | Input file path (file consumer) |
| `GEYSER_ENDPOINT` | — | gRPC endpoint URL (grpc consumer) |
| `GEYSER_TOKEN` | — | gRPC access token (grpc consumer) |
| `CIRCUIT_NARROW` | `0` | gRPC consumer: narrow the account subscription to registered pool vaults + 82-byte mints instead of every SPL token account. Cuts ~⅔ of account-stream egress. See [Subscription scope](#subscription-scope-grpc-cost-control). |
| `CIRCUIT_TX_WATCHLIST` | — | gRPC consumer: comma-separated pool addresses to receive the scoped transaction stream for (full-fidelity candles on low-volume pools). |
| `CIRCUIT_COST_PROBE` | `0` | Log a periodic breakdown of account-stream bytes by kind (vault / mint / discarded holder). Diagnostic only; safe to leave off. |

---

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

### Subscription scope (gRPC cost control)

Managed Geyser endpoints (Triton/Helius/QuickNode) bill by **egress bytes**, so what the indexer subscribes to is a direct cost lever. Pool-state accounts (Raydium CLMM/CPMM, Orca, PumpSwap) and Pump.fun bonding curves are always subscribed with `dataSize` filters that drop the large tick-array/observation accounts the parsers discard anyway.

The expensive part is SPL token accounts. CPMM and PumpSwap prices are derived from **pool vault balance deltas**, which requires watching the Token / Token-2022 programs. Subscribing to those programs *unfiltered* means receiving **every SPL token account on Solana** (~53 GB/day, of which ~⅔ are holder accounts the indexer never prices) — the "firehose."

Set **`CIRCUIT_NARROW=1`** to replace that unfiltered subscription with a precise one:

- the specific **registered pool vaults** (exactly the accounts that produce prices today), grown live every 8s as new pools register — `_maybeResubscribe()` re-sends the subscription with the updated vault set,
- **82-byte mints** for metadata, and
- the CPMM + Pump.fun pool states.

This cuts the account stream by ~⅔ (holder accounts are no longer received) with **no loss of priced tokens** — only vaults in the in-memory registry ever produced a price. Decimals for both mints of every pool are RPC-resolved at registration (so reverse-ordered pools price correctly), and `circuit:mint` metadata is back-filled from the same fetch for tokens the narrowed stream doesn't carry in-stream (e.g. token-2022 mints with extensions).

`CIRCUIT_NARROW` is **off by default** — the subscription is byte-for-byte the legacy one until you set it, so it can be enabled in production and rolled back with a single flag flip + restart. Use `CIRCUIT_COST_PROBE=1` to log the before/after byte breakdown.

---

## Redis data layout

| Key pattern | Type | TTL | Contents |
|---|---|---|---|
| `circuit:price:{mint}` | STRING | 30s | USD price record (stable-quoted pools) — `{ price, type, ts }` |
| `circuit:price-sol:{mint}` | STRING | 120s | SOL price record (SOL-quoted pools) |
| `circuit:pool:{account}` | STRING | 60s | Full pool state |
| `circuit:pool-by-mint:{mint}` | STRING | 24h | Reverse index — pool account address (pool addresses never change) |
| `circuit:mint:{mint}` | STRING | none | Mint metadata (decimals, supply, authorities) |
| `circuit:trending` | ZSET | rolling | score = accumulated swap volume (SOL), member = mint |
| `circuit:ph:{mint}` | LIST | 24h | Price history ring buffer (max 300 ticks) |
| `circuit:candles:1m:{mint}` | LIST | 4h | 1m OHLCV ring buffer (max 120, ~2h) |
| `circuit:candles:5m:{mint}` | LIST | 36h | 5m OHLCV ring buffer (max 288, ~24h) |
| `circuit:candles:1h:{mint}` | LIST | 8d | 1h OHLCV ring buffer (max 168, ~7d) |
| `circuit:candles:1d:{mint}` | LIST | 92d | 1d OHLCV ring buffer (max 90, ~90d) |

---

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

---

## Supported DEXes

| DEX | Program | Parser |
|---|---|---|
| Raydium AMM v4 | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` | `parsers/raydium.js` |
| Raydium CLMM | `CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK` | `parsers/raydium.js` |
| Orca Whirlpools | `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc` | `parsers/orca.js` |
| PumpSwap | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` | `parsers/pumpswap.js` |

Additional DEX parsers can be added by implementing the `processAccountEvent(event)` interface in `parsers/`.

---

## Deployment

### Stats logging

The indexer logs aggregate stats every 30 seconds — watch `errors` and `eps`:

```
circuit-indexer | Indexer stats { uptime: 5.0m, events: 42000, accounts: 38000,
  txns: 4000, slots: 450, pools: 1200, tokens: 80, candles: 24, errors: 0, eps: 140.0 }
```

### Graceful shutdown

Handles `SIGTERM` and `SIGINT`: flushes pending OHLCV candles, then disconnects Redis and Postgres cleanly.

### systemd

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

---

## Changelog

### v0.8.0
- **Narrowed Geyser subscription (`CIRCUIT_NARROW=1`)** — replaces the unfiltered Token/Token-2022 account subscription (every SPL token account, ~53 GB/day) with the specific registered pool vaults + 82-byte mints, grown live as pools register. Cuts ~⅔ of account-stream egress with no loss of priced tokens. Off by default (legacy subscription unchanged); single-flag rollback. See [Subscription scope](#subscription-scope-grpc-cost-control).
- **Fix: reverse-ordered PumpSwap decimals** — pools stored `base=WSOL, quote=token` defaulted the quote token's decimals to 9 ("assume WSOL") because only the base mint was RPC-resolved, mispricing those tokens 1000×. Now both pool mints are RPC-resolved and 9 is assumed only for the real WSOL mint. (Was masked under the full firehose, which pre-cached every mint's decimals.)
- **`circuit:mint` gap-fill** — mint metadata (decimals/supply/authorities) is back-filled from the pool-registration RPC fetch for tokens the narrowed stream doesn't carry in-stream, notably token-2022 mints with extensions.
- `CIRCUIT_COST_PROBE=1` diagnostic: periodic account-stream byte breakdown (vault / mint / discarded holder).

### v0.7.0
- Parses Raydium AMM v4 + CLMM, Orca Whirlpool, and PumpSwap pool state, plus swap-transaction buy/sell direction and SOL volume from token-balance deltas. Writes USD and SOL prices, pool state, a pool-by-mint reverse index (24h TTL — pool addresses are immutable), mint metadata, a trending ZSET, a price-history ring buffer, and 1m/5m/1h/1d OHLCV candle ring buffers to Redis; optional Postgres history for pools, tokens, and candles. Three input consumers (file, stdin, gRPC), 30s aggregate stats logging, and graceful flush-on-shutdown.

---

## Docs

- [Security policy](SECURITY.md) — disclosure process and operational safety notes
- [circuit-geyser](https://github.com/Circuit-LLM/circuit-geyser) — the validator plugin that produces the event stream this indexer consumes
- [OPS Terminal](https://circuitllm.xyz/data) — live source health, endpoint status, and stack stats

### Part of the Circuit stack

- [circuit-geyser](https://github.com/Circuit-LLM/circuit-geyser) — Agave validator Geyser plugin
- **circuit-indexer** — this repo, the stream consumer and data writer
- [circuit-node](https://github.com/Circuit-LLM/circuit-node) — RPC aggregator + data API
- [circuit-agent](https://github.com/Circuit-LLM/circuit-agent) — autonomous trading agent
- [circuitllm.xyz](https://circuitllm.xyz) — website and data terminal

---

## License

MIT — see [LICENSE](LICENSE)

---

## Community

- **X / Twitter:** [@CircuitLLM](https://x.com/CircuitLLM)
- **Telegram:** [t.me/circuitllm](https://t.me/circuitllm)
- **Website:** [circuitllm.xyz](https://circuitllm.xyz)
