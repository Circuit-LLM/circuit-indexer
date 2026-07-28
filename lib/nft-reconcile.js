// lib/nft-reconcile.js — Authoritative NFT listing reconciliation (Tensor ListState).
//
// The firehose (parsers/nft-tensor.js) only catches listings as they're CREATED/REPRICED — a slow
// trickle (~0.1/s), and it can't observe removals (Anchor `close` flips the owner). Listings are
// mostly static, so the firehose alone never holds the full ~120k stock. This job is the authority:
//
//   1. gPA snapshot of every ListState (Triton, off-Helius) → the full current listing set.
//   2. Refresh each per-asset key; expire keys no longer in the snapshot (sold/delisted).
//   3. Resolve assetId → collection (off-Helius, cache-forever, throttled backlog; cNFTs skipped).
//   4. Rebuild the per-collection listing ZSETs + floor snapshots from the resolved native listings.
//
// Runs only when CIRCUIT_NFT=1. All RPC uses lib/rpc-client (the indexer's Triton endpoint) — no Helius.
'use strict';

const { rpcCall } = require('./rpc-client');
const Logger      = require('./logger');
const redis       = require('../writers/redis');
const metaplex    = require('./metaplex');
const nftTensor   = require('../parsers/nft-tensor');

const LIST_DISC_B58 = 'ECt8xkbczt2'; // base58 of the ListState discriminator (JSON-RPC memcmp form)

// Bound the collection-resolution work per cycle so the one-time 120k backlog drains over several
// cycles instead of a single burst. Steady-state (after warm) only new mints need resolving.
const MAX_RESOLVE_PER_CYCLE = Number(process.env.CIRCUIT_NFT_RESOLVE_MAX) || 6000;

let _prevAssetIds = new Set();  // previous snapshot, for removal diffing (in-process)
let _running = false;

// One full reconciliation pass. opts.limit caps processed listings (for safe manual test runs).
async function reconcile(opts = {}) {
  if (_running) { Logger.warn('nft-reconcile: already running, skipping'); return null; }
  _running = true;
  const t0 = Date.now();
  try {
    // 1. Full ListState snapshot (owner + exact size + discriminator memcmp; JSON-RPC memcmp works).
    const res = await rpcCall('getProgramAccounts', [
      nftTensor.TENSOR_PROGRAM,
      {
        encoding: 'base64',
        dataSlice: { offset: 0, length: 90 },
        filters: [{ dataSize: nftTensor.LIST_SIZE }, { memcmp: { offset: 0, bytes: LIST_DISC_B58 } }],
      },
    ]);
    if (!Array.isArray(res)) { Logger.warn('nft-reconcile: gPA returned non-array'); return null; }

    // 2. Parse + refresh per-asset keys.
    let accounts = res;
    if (opts.limit) accounts = accounts.slice(0, opts.limit);
    const current = new Map();  // assetId → { assetId, priceSol, native }
    const batch = [];
    for (const a of accounts) {
      const b64 = a?.account?.data?.[0];
      if (!b64) continue;
      const parsed = nftTensor.parseListState(Buffer.from(b64, 'base64'));
      if (!parsed) continue;
      current.set(parsed.assetId, parsed);
      batch.push({ assetId: parsed.assetId, rec: {
        priceLamports: parsed.priceLamports, priceSol: parsed.priceSol,
        seller: parsed.seller, listState: a.pubkey, native: parsed.native,
      }});
    }
    await redis.writeNftListingsBatch(batch);  // pipelined refresh of all per-asset keys

    // expire removed (in snapshot last cycle, gone now)
    const gone = [..._prevAssetIds].filter(id => !current.has(id));
    await redis.removeNftListingsBatch(gone);
    const expired = gone.length;
    _prevAssetIds = new Set(current.keys());

    // 3. Collection resolution (native listings only — those with a comparable SOL price).
    const natives = [...current.values()].filter(v => v.native && v.priceSol > 0);
    const cachedColls = await redis.getCachedMintCollections(natives.map(v => v.assetId));
    const unresolved = [];
    const collOf = new Map();  // assetId → collection|'-'
    for (const v of natives) {
      const cached = cachedColls.get(v.assetId);
      if (cached === undefined) unresolved.push(v.assetId);
      else collOf.set(v.assetId, cached);
    }
    const toResolve = unresolved.slice(0, MAX_RESOLVE_PER_CYCLE);
    if (toResolve.length) {
      const resolved = await metaplex.resolveCollections(toResolve);
      for (const [mint, collection] of resolved) {
        await redis.cacheMintCollection(mint, collection);
        collOf.set(mint, collection || '-');
      }
    }

    // 4. Rebuild per-collection ZSETs + floors from every native listing with a known collection.
    const collectionMap = new Map();
    for (const v of natives) {
      const c = collOf.get(v.assetId);
      if (!c || c === '-') continue;
      if (!collectionMap.has(c)) collectionMap.set(c, []);
      collectionMap.get(c).push({ assetId: v.assetId, priceSol: v.priceSol });
    }
    await redis.rebuildNftCollections(collectionMap);

    const stats = {
      listings: current.size, expired, natives: natives.length,
      resolvedThisCycle: toResolve.length, backlog: Math.max(0, unresolved.length - toResolve.length),
      collections: collectionMap.size, ms: Date.now() - t0,
    };
    Logger.info('nft-reconcile: pass complete', stats);
    return stats;
  } catch (e) {
    Logger.error('nft-reconcile: failed', { error: e.message });
    return null;
  } finally {
    _running = false;
  }
}

// Schedule: first pass shortly after boot, then on an interval. Faster cadence while the collection
// backlog is still draining, then settle to steady-state.
function start() {
  const FAST_MS  = Number(process.env.CIRCUIT_NFT_RECONCILE_FAST_MS) || 3 * 60_000;
  const SLOW_MS  = Number(process.env.CIRCUIT_NFT_RECONCILE_SLOW_MS) || 30 * 60_000;
  let timer = null;
  const tick = async () => {
    const s = await reconcile();
    const delay = (s && s.backlog > 0) ? FAST_MS : SLOW_MS;  // drain fast, then settle
    timer = setTimeout(tick, delay);
    if (timer.unref) timer.unref();
  };
  timer = setTimeout(tick, 60_000);  // first pass ~60s after boot (let the stream warm first)
  if (timer.unref) timer.unref();
  Logger.info('nft-reconcile: scheduled', { fastMs: FAST_MS, slowMs: SLOW_MS });
}

module.exports = { reconcile, start };
