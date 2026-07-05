# Triton Geyser Egress Optimization — Plan

## OUTCOME (2026-07-05)
Built, tested, and live-flipped the multi-stream slicing — then **reverted to the original single stream**. Verdict: **the real (gzip-compressed, billed) wire savings from slicing is only ~10%, not the ~47% the uncompressed projection promised**, because gzip already compresses the low-entropy account tails and per-event overhead (pubkey/owner/framing) dominates small frequent accounts. Also ruled out: single global slice (Triton drops accounts < slice length), and the 8s vault re-subscribe (does NOT re-snapshot — measured 0.91×, not a lever). The indexer was already near its safe floor (transactions + holders previously cut). Bigger savings would require *event-count* reduction (poll-based pool discovery instead of streaming PumpSwap/CPMM pool-state, or trimming the pump.fun firehose) — which trades away new-token pricing latency the swarm depends on, so NOT pursued. **The slicing code remains, fully tested and env-gated (`CIRCUIT_STREAM_SLICING=1`), re-enablable in one command if ~10% is worth the 4-stream complexity.** Live state: original single stream, downstream verified healthy.

---

**Status:** COMPLETE — investigated, tested live, reverted. See OUTCOME above.
**Goal:** Cut Triton (yellowstone-grpc) egress — billed **pay-per-GB** — without disturbing any downstream consumer.
**Hard constraint:** the indexer's Redis outputs (`circuit:*`) must remain byte-identical. The trading swarm places real trades off this data; the website charts and paid data-api customers read it too.

---

## 1. Current state

- One long-lived bidi gRPC stream, `circuit-indexer` (systemd user unit `circuit-indexer.service`), endpoint `josheri-mainnet-70c9.mainnet.rpcpool.com:443` (Triton dedicated, **metered per-GB**).
- Measured egress ≈ **24.8 GB/day (~745 GB/mo)** (`/proc/<pid>/io` rchar over 3.27 days).
- Already optimized: **transactions off** (were ~76% of the firehose), **holder firehose narrowed to ~0%**, **gzip on** (`grpc.default_compression_algorithm=2`), commitment=Confirmed, blocks/blocksMeta off.
- Remaining egress is **load-bearing account data**: ~66% pool-state/bonding-curve bytes, ~34% vault+mint bytes.

## 2. The lever: `accountsDataSlice`

Every parser reads only a small **prefix** of each account and ignores the rest, but today the full account streams. Triton's `accountsDataSlice` (a global `{offset,length}[]` on the SubscribeRequest) makes the server send only the requested bytes.

- **Confirmed** supported + wire-serialized by our client `@triton-one/yellowstone-grpc` **v1.4.1**; currently `[]` (unused).
- **Global per subscription**, not per-filter → a single stream can carry only one slice length. Slicing pool states (need 333B) and pump.fun/vaults (need ≤72B) at their optima requires **two streams** (feasible: `subscribe()` returns a fresh multiplexed bidi stream from the same client).
- Re-subscription: on vault-registry growth the indexer rebuilds+rewrites the full SubscribeRequest every ~8s (Yellowstone replaces, not merges) — the slice must be included in that rebuild.

### Parser byte-usage catalog (verified)

| Parser / layout | on-chain size | **max byte read** | length guard | slice action |
|---|---|---|---|---|
| Raydium CLMM | 1544 | **273** | `<1544` | slice + **relax guard 1544→273** |
| Orca whirlpool | 653 | **213** | `<653` | slice + **relax guard 653→213** |
| Raydium CPMM | ~637 | **333** | `<333` | slice (guard already = max) |
| PumpSwap | 301 | **203** | `<301` | account ≤333, no change |
| Pump.fun bonding curve | ~150 | **49** | `<49` | Move B slice → 72 |
| Vault (SPL token acct, inline `indexer.js`) | 165 | **72** | `>=72` | Move B slice → 72 |
| SPL mint (`token.js`) | 82 | **82** | `<82` | **must keep 82** (freeze-authority is a conditional read at the very end) |
| Raydium AMM v4 | 752 | 328 | `<752` | **dormant — not subscribed**, ignore |

## 3. The change (data-driven — corrected after Phase 0b + Stage-A test)

The probe (§6) showed egress is dominated by **token-vault (40.5%)** and **PumpSwap pool-state (36.7%)** — *small* accounts that update very often — not the big CLMM accounts (8.2%).

> **CRITICAL CONSTRAINT (learned the hard way — Stage A, §6a):** Triton's `accountsDataSlice [{offset:0, length:N}]` is **not a truncation — it is a filter**. Any account whose data is **shorter than N is DROPPED from the stream entirely** (verified live: a global `[0,333]` slice silently stopped PumpSwap/vault/pump.fun/mint pricing). Therefore a single global slice is impossible, and **each stream's slice length must be ≤ the smallest account it carries**, and ≥ the max byte its parsers read. This forces owner-partitioned streams.

Real on-chain sizes (measured, `tools/acct-sizes.js`): CLMM 1544 · Orca 653 · CPMM 637 · PumpSwap 301 · **vault (SPL/Token-2022) 165–594** · **pump.fun curve 115–151** · mint 82. Each stream's slice sits below its members' minimum size and above their parsers' read length:

| Stream | slice | carries | min acct size | parser needs | egress share | est. saved |
|---|---|---|---|---|---|---|
| **A** | **72** | vaults + pump.fun curves | 115 | 72 | 45.2% | **~25%** |
| **B** | **215** | PumpSwap + Orca | 301 | 213 | 40.0% | **~13%** |
| **C** | **333** | CLMM + CPMM | 637 | 333 | 13.3% | **~9%** |
| **D** | **none** | mints (82B; can't slice below their 82-byte read) | 82 | 82 | 1.5% | 0 |

- Contiguous `[0,N]` slices ⇒ all parser offsets preserved (no offset math changes); only unread trailing bytes are dropped.
- Guard relaxations required: **CLMM `1544→273`, Orca `653→213`** (their accounts arrive < full size and would otherwise be silently dropped). CPMM (333), PumpSwap (301≤ its stream slice), pump.fun (49), vault (72), mint (82) guards are already satisfied.
- **Priority order (by measured value), each stage independently valuable + verifiable:**
  1. **Stream A (vault + pump.fun → 72): ~25% (~6.2 GB/day)** — the single biggest win.
  2. **Stream B (PumpSwap + Orca → 215): ~13% (~3.2 GB/day)** — captures the #2 giant, which a naive 333 global slice misses entirely (PumpSwap is 301B < 333).
  3. **Stream C (CLMM + CPMM → 333): ~9% (~2.2 GB/day)** — the minor pool states (this was the original "single-slice Move A", now the *lowest*-value move).
- Combined ceiling ≈ **47% (~24.8 → ~13 GB/day)**. (Uncompressed-byte projection; the wire is gzipped, so the real GB drop is measured post-flip — §4.)

### Implementation note
The current `GrpcConsumer` handles one stream; this needs a refactor to N streams each `{filters, slice}` from config. Only Stream A (vaults) grows → needs the 8s re-subscribe; B/C are static owner filters. **Each stream must reconnect/resubscribe as robustly as today's single stream** — a dead stream = stale slice of data; this reliability parity is part of the work, not just correctness.

### Minor lever
- `slots` currently stream at all commitments; `filterByCommitment: true` trims to Confirmed. Tiny; bundle in.

## 4. Verification ladder (how we guarantee no downstream disruption)

- **Phase 0a — offline parser proof. ✅ DONE / PASS.** `tools/verify-slice.js` fetched ~1,700 real live accounts (pools from `circuit:pool:*`, vaults from `circuit:vault-registry`, mints from `circuit:mint:*`) via RPC and asserted `parse(full) == parse(bytes-past-slice-zeroed)` for every one. Result: **all identical** (CLMM 84/84, Orca 105/105, CPMM/PumpSwap 354/354, pump.fun 400/400, vault 293/293, mint 179/179). The truncated-length variant lit up **only** CLMM+Orca as needing a guard relax — exactly matching the catalog. Read-only; no production impact.
- **Phase 0b — per-program byte probe.** Cost probe (`CIRCUIT_COST_PROBE=1`) extended to attribute streamed bytes per program and project Move A/B savings (`Cost probe: per-program egress` log line). Tells us whether Move A or Move B is the bigger prize before writing either.
- **Shadow canary (pre-cutover).** Run a second sliced indexer against a separate Redis DB and diff its `circuit:*` outputs against live across all pool types for ~1h. Zero production impact.
- **Env-gated flip + instant rollback.** Put slice length + guard values behind env vars (defaults = current behavior). Cutover = restart; rollback = restart with flag off (seconds; `Restart=always`).
- **Post-flip parity watch.** Before/after counts of `circuit:price:*`, trending size, candle freshness; one spot-priced known-live token per pool type (CLMM/Orca/CPMM/PumpSwap/pump.fun); swarm health + chart freshness.
- **Measure the real GB drop** via `/proc/<pid>/io` rchar rate before vs after.

## 5. Blast radius (what a regression would hit)

| Consumer | reads | exposure |
|---|---|---|
| **circuit-price-feed** :18941 | price, price-sol, pool, pool-by-mint, mint, trending, ph, **candles (sole reader)** | FREE; fan-out hub for everything below |
| **trading swarm** (circuit-swarm ×10) | price-feed prices/candles/scan | **highest consequence — real trades**; stale data strands positions / bad entries |
| **website** (public) | charts (`circuit:candles` first), trending, scan, CIRC price | user-visible; has Gecko/node fallback for charts |
| **circuit-data-api** :18960 | proxies price-feed + node | **paid x402 customers** (`/api/token-price`, `/api/market-overview`, …) |
| **circuit-node** :18940 | price, mint, pool, trending | paid analytics; priceTracker samples price every 30s |
| **node network** (node-client/clients) | trending + scan | widest fan-out, 60s cached, lowest severity |

Keys by blast radius: `circuit:price` > `circuit:pool`/`pool-by-mint` > `circuit:candles` > `circuit:trending` > `circuit:price-sol` > `circuit:mint` > `circuit:ph`.

## 6. Sequence & status

- [x] Phase 0a — offline parser proof (PASS at exact design slices 72/215/333/82)
- [x] Phase 0b — per-program byte probe (stable ratios; results below)
- [ ] Phase 1 — refactor `GrpcConsumer` to N streams; **Stream A (vault+pump.fun → 72)** first, env-gated → shadow canary → flip → parity watch → measure GB drop
- [ ] Phase 2 — **Stream B (PumpSwap+Orca → 215)**, same verification ladder
- [ ] Phase 3 — **Stream C (CLMM+CPMM → 333)** + `slots.filterByCommitment`

### Phase 0b results (4 samples, 60–240s, ratios stable within ±0.5pt)

Per-program egress share of total account bytes:

| program | share | ≈ GB/day (of 24.8) | account → parser max-read |
|---|---|---|---|
| token-vault | 40.5% | 10.0 | 165 → 72 |
| PumpSwap | 36.7% | 9.1 | 301 → 203 |
| CLMM | 8.2% | 2.0 | 1544 → 273 |
| CPMM | 5.1% | 1.3 | 637 → 333 |
| pump.fun | 4.7% | 1.2 | 150 → 49 |
| Orca | 3.3% | 0.8 | 653 → 213 |
| token-mint | 1.5% | 0.4 | 82 |

Probe-projected savings: single 333 slice **~13%** · vault+pump.fun→72 **~25.8%** · both **~38.7%**. The 3-stream design adds PumpSwap→215 for **~47%** total (the 333 slice alone can't touch PumpSwap's 36.7% because 301 < 333).

## 7. Notes / rollback
- The probe edit (`indexer.js`, per-program attribution) is purely additive inside the `CIRCUIT_COST_PROBE` block — no effect when the flag is off. Can be reverted after measurement or kept as a diagnostic.
- `tools/verify-slice.js` is an untracked, read-only verification tool (safe to keep or delete).
- No subscription/slice change has been applied to production. Move A/B require explicit go-ahead.
