# Indexer-Integrated Historical Candle Backfill — Spec

Status: **DRAFT for review — nothing implemented.** Repo: `circuit-indexer`.
Goal: reconstruct any pool's *pre-tracking* candle history from the chain, computed
**identically** to live candles, fully self-reliant (no GeckoTerminal), so charts and
`token-ohlcv` have full depth for tokens we started indexing after their launch.

---

## Is it dangerous?

**The one real danger:** the indexer is a critical **single point of failure** — the sole
writer to Redis (live prices the whole stack reads) and Postgres. Anything that slows or
stalls it degrades everything (the exact lull/staleness problem we just fixed on the read
side). Everything else is low-risk *because historical transactions are immutable*
(finalized → no reorg) and we write **gap-only, never overwriting** live data.

So it's dangerous **only if built naively** (in-process, unthrottled, overwriting). Built
to the controls below, it is low-risk.

| Risk | Mitigation |
|---|---|
| Blocking the live indexer's event loop | **Separate process.** Backfill imports the indexer's *code*, never runs inside its live runtime. |
| RPC contention starving live indexing / 429s | **Throttle** (low concurrency, backoff); ideally a **separate RPC key**; run off-peak. |
| Overwriting / corrupting live candles | Writes are **`ON CONFLICT (time,mint,tf) DO NOTHING`** — fills only missing bars. Never `UPDATE`. |
| Corrupting the live aggregator's buckets | Backfill uses its **own `OHLCVAggregator` instance** + its own write callback. Zero shared state. |
| Postgres lock/slow on the live writer | Small batched inserts (short locks); **stage table first**, promote gap-only. |
| Bad/inconsistent reconstruction (a "seam") | **Reuse `handleTransaction` + `OHLCVAggregator`** — the live code path — so bars are byte-identical; plus a **parity gate** before promotion. |
| Failed/again-counted txns | Skip `meta.err`; dedup by signature — same guards `handleTransaction` already has. |

---

## Architecture: share the CODE, isolate the RUNTIME

The parity win and the safety win are the same decision: **reuse the live path, run it
out-of-process.**

- **Reuse `lib/ohlcv.js::OHLCVAggregator`** — feed reconstructed ticks
  `tick(mint, price, volSol, ts, isBuy)`; it emits the same OHLCV bars the live path does.
- **Reuse the txn→tick logic in `handleTransaction`** (indexer.js:550–598): pre/post token
  balances → price → volume → direction → `ohlcv.tick(...)`. Backfill feeds it *historical*
  transactions instead of the Geyser stream.
- **Run as a standalone runner** (`scripts/backfill.js`), its own process, its own
  aggregator whose emit-callback writes to a **staging table** (not the live path).

### Prerequisite refactor (small)
Extract the pure "transaction → tick(s)" computation out of `handleTransaction` into an
exported helper (e.g. `lib/replay.js::ticksFromTransaction(tx, poolMeta)`), so **both** the
live handler and the backfill runner call the identical function. No behavior change to
live; it just becomes importable. This is what guarantees no seam.

---

## Flow

1. **Resolve pool** — decode vaults/mints/decimals (reuse `parsers/pumpswap.js`).
2. **Page history** — `getSignaturesForAddress(pool)` back to pool creation (or a
   `--since-slot`), throttled; checkpoint the cursor for resumability.
3. **Replay** — `getTransaction` (throttled, concurrency-capped) → `ticksFromTransaction`
   → a backfill-owned `OHLCVAggregator` → emit candles to a **staging table**
   (`ohlcv_candles_staging`, same schema).
4. **Parity gate** — for the window that overlaps live data, compare staging vs
   `ohlcv_candles`; require agreement within tolerance (the PoC's cross-check, automated).
   Abort promotion on divergence.
5. **Promote (gap-only)** — `INSERT INTO ohlcv_candles SELECT ... FROM staging
   ON CONFLICT (time,mint,tf) DO NOTHING`. Only pre-tracking hours land; live rows are
   never touched. Drop staging.
6. **Serve with gap-fill-on-read** — at chart read time, carry forward the last close
   through no-trade hours (our candles are trade-only; this matches Gecko's continuous look
   without storing synthetic bars).

---

## Scope / rollout

- **Phase 1 — one-time sweep** for existing gaps: CIRC + priority tokens, run manually,
  validated per token. (CIRC PoC already proved: ~10k txns, ~15 min, ~1 MB, back to the
  05-28 pool-graduation.)
- **Phase 2 (optional) — on-first-seen**: when the indexer registers a *new* pool, enqueue
  a backfill job to a durable queue; a **separate worker** drains it (still out-of-process).
  Future tokens auto-backfill from launch.
- **Pre-graduation history** (e.g. CIRC 05-23→05-28 on the pump.fun bonding curve): a
  separate replay using `parsers/pumpfun.js`; optional, low value.

---

## Edge cases

- **Immutable history** → no reorg handling needed; skip `meta.err` txns.
- **Multi-swap / routed txns** → handled by reusing `handleTransaction`'s logic.
- **Unknown decimals** (exotic quote) → skip rather than misprice (existing indexer guard).
- **Pool graduation boundary** → backfill the PumpSwap pool; bonding-curve phase is separate.
- **Trade-only sparsity** → not a data defect; addressed by gap-fill-on-read at serving.

---

## Testing / acceptance

- **Parity gate is the acceptance test**: reconstructed overlap must match live within
  tolerance, per token, before promotion.
- **Live-safety check**: run a backfill while watching live candle freshness
  (`circuit:price-sol:*` ages, indexer stats) — confirm no regression in live indexing.
- **Idempotent**: re-running produces no duplicate/changed rows (DO NOTHING).

---

## Already de-risked (CIRC PoC, 2026-07-02)

Feasible ✓ (archival RPC reaches launch), correct ✓ (reconstruction matched live within
~2% on well-sampled hours), cheap ✓ (~10k txns / ~15 min), trivial space ✓ (~1 MB), full
range ✓ (05-28→now, 615 1h bars vs Gecko's 746 — the delta is no-trade hours). The one-off
lives in the isolated `ohlcv_backfill` table and should be **dropped** once this ships —
it used an inline reconstruction, not the live code path, so it is *not* promotion-grade.
