// writers/redis.js — Write hot price data to Redis.
//
// Redis data structures:
//   circuit:price:{mint}          STRING  JSON USD price record, TTL 30s (stable-quoted pools only)
//   circuit:price-sol:{mint}      STRING  JSON SOL price record, TTL 120s (SOL-quoted pools)
//   circuit:pool:{poolAccount}    STRING  JSON pool state, TTL 60s
//   circuit:pool-by-mint:{mint}   STRING  poolAccount address, TTL 120s (reverse index)
//   circuit:mint:{mint}           STRING  JSON mint metadata, TTL 14d (hot cache; durable copy in Postgres `tokens`)
//   circuit:trending              ZSET    score=accumulated volume (SOL), member=mint
//   circuit:ph:{mint}             LIST    price history ring buffer, max 300 entries, TTL 24h
//                                         each entry: JSON {p, ts} (priceSol, unix ms)
//   circuit:candles:1m:{mint}     LIST    1m OHLCV ring buffer, max 120 candles (~2h), TTL 4h
//   circuit:candles:5m:{mint}     LIST    5m OHLCV ring buffer, max 288 candles (~24h), TTL 36h
//   circuit:candles:1h:{mint}     LIST    1h OHLCV ring buffer, max 168 candles (~7d), TTL 8d
//   circuit:candles:1d:{mint}     LIST    1d OHLCV ring buffer, max 90 candles (~90d), TTL 92d
//                                         each entry: JSON {t, o, h, l, c, v, n, b, s}
//                                           t=openTime ms, o/h/l/c=OHLCV, n=ticks, b=buys, s=sells
//   circuit:nft:listing:{assetId} STRING  JSON Tensor listing {priceSol,priceLamports,seller,listState,native}, TTL 2h
//                                         (firehose delta; hourly gPA reconciliation is authoritative — CIRCUIT_NFT=1)
//   circuit:nft:mint-collection:{mint} STRING  collection key (or '-' = none), TTL 30d (immutable; resolved off-Helius)
//   circuit:nft:listings:{collection}  ZSET    score=priceSol member=assetId (native listings; rebuilt each reconcile)
//   circuit:nft:floor:{collection}     STRING  JSON { collection, floorSol, listed, ts }
//   circuit:nft:bids:{collection}      ZSET    score=priceSol member=bidState (collection-wide bids; top = ZREVRANGE 0 0)
//   circuit:nft:coll-name:{collection} STRING  collection human name (or '-'), 30d TTL (for name search)
//
// Requires Redis ≥ 6.2. Install: sudo apt-get install -y redis-server
// This module is a no-op if Redis is not available.
'use strict';

const Logger = require('../lib/logger');

const PRICE_TTL        = 30;    // seconds — USD price records
const PRICE_SOL_TTL    = 120;   // seconds — SOL price records
const POOL_TTL         = 60;    // seconds — pool state
const POOL_BY_MINT_TTL = 86400; // seconds — reverse index (24h: pool addresses don't change)

// Price history ring buffer config
const PH_MAX_ENTRIES   = 300;   // ~5 min at 1 tick/sec for active pools
const PH_TTL           = 86400; // 24h TTL

// Mint metadata cache TTL. Immutable on-chain data with a durable copy in Postgres `tokens`
// (data-api falls back to it), so this only bounds the hot cache. writeMint runs on every
// trade/registration, so any actively- or occasionally-traded token keeps refreshing its TTL;
// only long-dead tokens expire. Caps circuit:mint growth (the only unbounded key class) below
// the Redis maxmemory ceiling. 14d comfortably covers the active working set.
const MINT_TTL         = 14 * 86400; // 14 days

// Candle ring buffer config (max entries per window, TTL seconds)
const CANDLE_CFG = {
  '1m': { max: 120, ttl: 4   * 3600 },
  '5m': { max: 288, ttl: 36  * 3600 },
  '1h': { max: 168, ttl: 8   * 86400 },
  '1d': { max: 90,  ttl: 92  * 86400 },
};

let _client = null;

async function getClient() {
  if (_client) return _client;
  let ioredis;
  try {
    ioredis = require('ioredis');
  } catch {
    Logger.warn('RedisWriter: ioredis not installed — running in no-op mode. npm install ioredis');
    return null;
  }
  const url  = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
  _client    = new ioredis(url, { lazyConnect: true, maxRetriesPerRequest: 1 });
  try {
    await _client.connect();
    Logger.info('RedisWriter: connected', { url: url.replace(/\/\/[^@]*@/, '//***@') });
  } catch (e) {
    Logger.warn('RedisWriter: could not connect to Redis — running in no-op mode', { error: e.message });
    _client = null;
  }
  return _client;
}

// ── Write operations ──────────────────────────────────────────────────────────

async function writePrice(mint, priceUsd, source, extraFields = {}) {
  const r = await getClient();
  if (!r) return;
  const record = {
    mint,
    priceUsd,
    source,
    ts: Date.now(),
    ...extraFields,
  };
  await r.setex(`circuit:price:${mint}`, PRICE_TTL, JSON.stringify(record));
}

async function writePool(poolAccount, poolState) {
  const r = await getClient();
  if (!r) return;
  await r.setex(`circuit:pool:${poolAccount}`, POOL_TTL, JSON.stringify({
    ...poolState,
    updatedAt: Date.now(),
  }));
}

async function writeMint(mint, mintData) {
  const r = await getClient();
  if (!r) return;
  // 14d TTL, refreshed on every write; durable copy lives in Postgres `tokens` (see MINT_TTL).
  await r.setex(`circuit:mint:${mint}`, MINT_TTL, JSON.stringify({
    ...mintData,
    indexedAt: Date.now(),
  }));
}

// SOL-quoted price: priceSol = SOL per 1 UI token (decimal-adjusted).
// Written for any pool where one side is SOL — this is what circuit-agents consume.
// extraFields may include: poolAccount, coinReserve, pcReserve, coinDecimals, pcDecimals
async function writePriceSol(mint, priceSol, source, extraFields = {}) {
  const r = await getClient();
  if (!r) return;
  const record = { mint, priceSol, source, ts: Date.now(), ...extraFields };
  await r.setex(`circuit:price-sol:${mint}`, PRICE_SOL_TTL, JSON.stringify(record));
}

// Reverse index: mint → pool account address.
// Allows price-feed to resolve a mint to its pool without scanning all pool keys.
// Also written for Pump.fun tokens registered via circuit-price-feed /register endpoint.
async function writePoolByMint(mint, poolAccount) {
  const r = await getClient();
  if (!r) return;
  await r.setex(`circuit:pool-by-mint:${mint}`, POOL_BY_MINT_TTL, poolAccount);
}

async function updateTrending(mint, volumeSolDelta) {
  const r = await getClient();
  if (!r) return;
  await r.zincrby('circuit:trending', volumeSolDelta, mint);
}

// Append a single price tick to the per-mint price history ring buffer.
// Called by indexer every time writePriceSol produces a fresh price.
async function appendPriceHistory(mint, priceSol, ts) {
  const r = await getClient();
  if (!r) return;
  const key    = `circuit:ph:${mint}`;
  const entry  = JSON.stringify({ p: priceSol, ts });
  const pipe   = r.pipeline();
  pipe.lpush(key, entry);
  pipe.ltrim(key, 0, PH_MAX_ENTRIES - 1);
  pipe.expire(key, PH_TTL);
  await pipe.exec();
}

// Write a completed OHLCV candle to the per-mint ring buffer for the given window.
// Called from the indexer's onCandle callback.
// Idempotent candle write. A bucket is re-emitted many times over its life (live
// snapshots of the open candle); each emission is a full cumulative snapshot. Update
// the head entry in place when the openTime matches instead of appending a duplicate —
// lightweight-charts and the agent scorer require one entry per bucket (strictly
// ascending unique time). Atomic via Lua because writeCandleBuffer is fire-and-forget,
// so two writes for the same key could otherwise race on the read-modify-write.
const CANDLE_WRITE_LUA = `
local head = redis.call('LINDEX', KEYS[1], 0)
if head then
  local ht = tonumber(string.match(head, '^{"t":(%-?%d+)'))
  local nt = tonumber(ARGV[2])
  if ht == nt then redis.call('LSET', KEYS[1], 0, ARGV[1])
  elseif nt < ht then return 0
  else redis.call('LPUSH', KEYS[1], ARGV[1]) end
else
  redis.call('LPUSH', KEYS[1], ARGV[1])
end
redis.call('LTRIM', KEYS[1], 0, tonumber(ARGV[3]) - 1)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
return 1`;

async function writeCandleBuffer(candle) {
  const cfg = CANDLE_CFG[candle.window];
  if (!cfg) return; // unsupported window
  const r = await getClient();
  if (!r) return;
  const key   = `circuit:candles:${candle.window}:${candle.mint}`;
  const entry = JSON.stringify({
    t: candle.openTime,
    o: candle.open,
    h: candle.high,
    l: candle.low,
    c: candle.close,
    v: candle.volume,
    n: candle.ticks,
    b: candle.buys  ?? 0,
    s: candle.sells ?? 0,
  });
  await r.eval(CANDLE_WRITE_LUA, 1, key, entry, String(candle.openTime), String(cfg.max), String(cfg.ttl));
}

// ── Read operations ───────────────────────────────────────────────────────────

async function getPrice(mint) {
  const r = await getClient();
  if (!r) return null;
  const raw = await r.get(`circuit:price:${mint}`);
  return raw ? JSON.parse(raw) : null;
}

async function getPriceSol(mint) {
  const r = await getClient();
  if (!r) return null;
  const raw = await r.get(`circuit:price-sol:${mint}`);
  return raw ? JSON.parse(raw) : null;
}

async function getPool(poolAccount) {
  const r = await getClient();
  if (!r) return null;
  const raw = await r.get(`circuit:pool:${poolAccount}`);
  return raw ? JSON.parse(raw) : null;
}

async function getPoolByMint(mint) {
  const r = await getClient();
  if (!r) return null;
  return await r.get(`circuit:pool-by-mint:${mint}`);
}

async function getMint(mint) {
  const r = await getClient();
  if (!r) return null;
  const raw = await r.get(`circuit:mint:${mint}`);
  return raw ? JSON.parse(raw) : null;
}

async function getTrending(limit = 20) {
  const r = await getClient();
  if (!r) return [];
  const raw = await r.zrevrange('circuit:trending', 0, limit - 1, 'WITHSCORES');
  const out = [];
  for (let i = 0; i < raw.length; i += 2) {
    out.push({ mint: raw[i], volumeSol: parseFloat(raw[i + 1]) });
  }
  return out;
}

// Returns price history for a mint as [{p, ts}, ...] oldest-first, up to `limit` entries.
async function getPriceHistory(mint, limit = 100) {
  const r = await getClient();
  if (!r) return [];
  const raw = await r.lrange(`circuit:ph:${mint}`, 0, limit - 1);
  // LPUSH stores newest first; reverse to return oldest-first
  return raw.map(e => { try { return JSON.parse(e); } catch { return null; } })
            .filter(Boolean)
            .reverse();
}

// Returns candle ring buffer for a mint+window, oldest-first, up to `limit` candles.
async function getCandles(mint, window, limit = 100) {
  const cfg = CANDLE_CFG[window];
  if (!cfg) return [];
  const r = await getClient();
  if (!r) return [];
  const cap = Math.min(limit, cfg.max);
  const raw = await r.lrange(`circuit:candles:${window}:${mint}`, 0, cap - 1);
  return raw.map(e => { try { return JSON.parse(e); } catch { return null; } })
            .filter(Boolean)
            .reverse();
}

async function disconnect() {
  if (_client) { await _client.quit(); _client = null; }
}

// ── NFT listings (Tensor marketplace) ───────────────────────────────────────────
// Two-channel model (see parsers/nft-tensor.js):
//   • firehose delta — writeNftListing() on every ListState create/reprice (this file, real-time)
//   • hourly reconciliation — a full off-Helius gPA snapshot rebuilds the authoritative set,
//     refreshes TTLs, and expires sold/delisted accounts (Anchor `close` flips the owner, so the
//     stream can't observe the removal). Reconciliation also resolves assetId→collection.
// Per-asset record TTL is generous so a still-open (un-repriced) listing survives between hourly
// snapshots; the snapshot refreshes it. Collection floor bucketing lives in circuit:nft:listings:{collection}
// (a ZSET, score=priceSol) written once collection is resolvable — added with the reconciliation pass.
const NFT_LISTING_TTL = 2 * 3600; // seconds (2h — comfortably spans the hourly reconciliation)

// Per-asset listing record. assetId → {priceSol, priceLamports, seller, listState, native, ts}
async function writeNftListing(assetId, rec) {
  const r = await getClient();
  if (!r) return;
  await r.setex(`circuit:nft:listing:${assetId}`, NFT_LISTING_TTL, JSON.stringify({ ...rec, ts: Date.now() }));
}

async function getNftListing(assetId) {
  const r = await getClient();
  if (!r) return null;
  try { const j = await r.get(`circuit:nft:listing:${assetId}`); return j ? JSON.parse(j) : null; }
  catch { return null; }
}

// Batched per-asset writes for the reconciliation snapshot (pipelined — one round-trip per chunk,
// not one per listing). records: [{ assetId, rec }]
async function writeNftListingsBatch(records) {
  const r = await getClient();
  if (!r) return;
  try {
    const CHUNK = 5000;
    for (let i = 0; i < records.length; i += CHUNK) {
      const pipe = r.pipeline();
      for (const { assetId, rec } of records.slice(i, i + CHUNK)) {
        pipe.setex(`circuit:nft:listing:${assetId}`, NFT_LISTING_TTL, JSON.stringify({ ...rec, ts: Date.now() }));
      }
      await pipe.exec();
    }
  } catch (e) { Logger.warn('writeNftListingsBatch failed', { error: e.message }); }
}

// Batched removals (expired listings).
async function removeNftListingsBatch(assetIds) {
  if (!assetIds || !assetIds.length) return;
  const r = await getClient();
  if (!r) return;
  try {
    const CHUNK = 5000;
    for (let i = 0; i < assetIds.length; i += CHUNK) {
      const pipe = r.pipeline();
      for (const id of assetIds.slice(i, i + CHUNK)) pipe.del(`circuit:nft:listing:${id}`);
      await pipe.exec();
    }
  } catch (e) { Logger.warn('removeNftListingsBatch failed', { error: e.message }); }
}

// Remove a listing (called by the reconciliation diff when an account is gone from the snapshot).
async function removeNftListing(assetId) {
  const r = await getClient();
  if (!r) return;
  try { await r.del(`circuit:nft:listing:${assetId}`); } catch {}
}

// mint → collection cache. Immutable membership, so a long TTL (refreshed by reconciliation) just
// bounds the key class. `null` collection (not a regular NFT / no collection) is cached as the
// sentinel '-' so we never re-resolve it. circuit:nft:mint-collection:{mint}
const NFT_MINT_COLL_TTL = 30 * 86400;

async function cacheMintCollection(mint, collection) {
  const r = await getClient();
  if (!r) return;
  try { await r.setex(`circuit:nft:mint-collection:${mint}`, NFT_MINT_COLL_TTL, collection || '-'); } catch {}
}

async function getCachedMintCollection(mint) {
  const r = await getClient();
  if (!r) return undefined;                        // undefined = not cached; '-'/null-string handled by caller
  try {
    const v = await r.get(`circuit:nft:mint-collection:${mint}`);
    return v === null ? undefined : v;             // '-' means resolved-to-none
  } catch { return undefined; }
}

// Batched MGET variant for reconciliation. Returns Map(mint → cachedValue|undefined).
async function getCachedMintCollections(mints) {
  const out = new Map();
  const r = await getClient();
  if (!r) { mints.forEach(m => out.set(m, undefined)); return out; }
  try {
    const CHUNK = 5000;
    for (let i = 0; i < mints.length; i += CHUNK) {
      const slice = mints.slice(i, i + CHUNK);
      const vals = await r.mget(...slice.map(m => `circuit:nft:mint-collection:${m}`));
      slice.forEach((m, j) => out.set(m, vals[j] === null ? undefined : vals[j]));
    }
  } catch { mints.forEach(m => { if (!out.has(m)) out.set(m, undefined); }); }
  return out;
}

// collection → human name cache (from the collection NFT's metadata). '-' = unresolved.
// circuit:nft:coll-name:{collection}
async function cacheCollName(collection, name) {
  const r = await getClient();
  if (!r) return;
  try { await r.setex(`circuit:nft:coll-name:${collection}`, NFT_MINT_COLL_TTL, name || '-'); } catch {}
}

async function getCachedCollNames(collections) {
  const out = new Map();
  const r = await getClient();
  if (!r) { collections.forEach((c) => out.set(c, undefined)); return out; }
  try {
    const CHUNK = 5000;
    for (let i = 0; i < collections.length; i += CHUNK) {
      const slice = collections.slice(i, i + CHUNK);
      const vals = await r.mget(...slice.map((c) => `circuit:nft:coll-name:${c}`));
      slice.forEach((c, j) => out.set(c, vals[j] === null ? undefined : vals[j]));
    }
  } catch { collections.forEach((c) => { if (!out.has(c)) out.set(c, undefined); }); }
  return out;
}

// whitelist → voc (collection mint) cache. Immutable; '-' sentinel = no voc (merkle/fvc whitelist).
// circuit:nft:wl-voc:{whitelist}
async function cacheWhitelistVoc(whitelist, voc) {
  const r = await getClient();
  if (!r) return;
  try { await r.setex(`circuit:nft:wl-voc:${whitelist}`, NFT_MINT_COLL_TTL, voc || '-'); } catch {}
}

async function getCachedWhitelistVocs(whitelists) {
  const out = new Map();
  const r = await getClient();
  if (!r) { whitelists.forEach(w => out.set(w, undefined)); return out; }
  try {
    const CHUNK = 5000;
    for (let i = 0; i < whitelists.length; i += CHUNK) {
      const slice = whitelists.slice(i, i + CHUNK);
      const vals = await r.mget(...slice.map(w => `circuit:nft:wl-voc:${w}`));
      slice.forEach((w, j) => out.set(w, vals[j] === null ? undefined : vals[j]));
    }
  } catch { whitelists.forEach(w => { if (!out.has(w)) out.set(w, undefined); }); }
  return out;
}

// Rebuild the per-collection listing ZSETs + floor snapshots from the reconciliation snapshot.
//   collectionMap: Map(collection → [{ assetId, priceSol }])   (native-SOL listings only)
// circuit:nft:listings:{collection}  ZSET  score=priceSol member=assetId
// circuit:nft:floor:{collection}     STRING JSON { collection, floorSol, listed, ts }
// Collections present before but absent now are dropped, so the floor set stays accurate.
async function rebuildNftCollections(collectionMap) {
  const r = await getClient();
  if (!r) return;
  try {
    // existing collection keys → drop any not in the new snapshot
    const existing = new Set();
    let cur = '0';
    do {
      const [next, batch] = await r.scan(cur, 'MATCH', 'circuit:nft:listings:*', 'COUNT', 1000);
      cur = next;
      for (const k of batch) existing.add(k.slice('circuit:nft:listings:'.length));
    } while (cur !== '0');

    const pipe = r.pipeline();
    for (const [collection, items] of collectionMap) {
      const key = `circuit:nft:listings:${collection}`;
      pipe.del(key);
      // ZADD in one shot: [score, member, score, member, ...]
      const args = [];
      let floor = Infinity;
      for (const it of items) { args.push(it.priceSol, it.assetId); if (it.priceSol < floor) floor = it.priceSol; }
      if (args.length) pipe.zadd(key, ...args);
      pipe.set(`circuit:nft:floor:${collection}`,
        JSON.stringify({ collection, floorSol: floor === Infinity ? null : floor, listed: items.length, ts: Date.now() }));
      existing.delete(collection);
    }
    // stale collections (had listings, none now)
    for (const gone of existing) { pipe.del(`circuit:nft:listings:${gone}`); pipe.del(`circuit:nft:floor:${gone}`); }
    await pipe.exec();
  } catch (e) { Logger.warn('rebuildNftCollections failed', { error: e.message }); }
}

// Rebuild per-collection BID ZSETs from the reconciliation snapshot (collection-wide bids only).
//   bidMap: Map(collection → [{ bidState, priceSol }])   (native-SOL bids)
// circuit:nft:bids:{collection}  ZSET  score=priceSol member=bidState  (top bid = ZREVRANGE 0 0)
async function rebuildNftBids(bidMap) {
  const r = await getClient();
  if (!r) return;
  try {
    const existing = new Set();
    let cur = '0';
    do {
      const [next, batch] = await r.scan(cur, 'MATCH', 'circuit:nft:bids:*', 'COUNT', 1000);
      cur = next;
      for (const k of batch) existing.add(k.slice('circuit:nft:bids:'.length));
    } while (cur !== '0');

    const pipe = r.pipeline();
    for (const [collection, items] of bidMap) {
      const key = `circuit:nft:bids:${collection}`;
      pipe.del(key);
      const args = [];
      for (const it of items) args.push(it.priceSol, it.bidState);
      if (args.length) pipe.zadd(key, ...args);
      existing.delete(collection);
    }
    for (const gone of existing) pipe.del(`circuit:nft:bids:${gone}`);
    await pipe.exec();
  } catch (e) { Logger.warn('rebuildNftBids failed', { error: e.message }); }
}

// ── Vault registry persistence ──────────────────────────────────────────────────
// The in-memory vaultRegistry rebuilds from ~zero over ~1.5h after a restart (CPMM/PumpSwap pool-state
// accounts stream slowly), degrading pricing + discovery coverage for that whole window. Mirror it to a
// Redis hash so a restart rehydrates it instantly. Dead pools emit ~no vault updates, so the larger
// subscription costs ~nothing in Triton egress.
const VAULT_REGISTRY_KEY = 'circuit:vault-registry';

async function saveVaultEntry(vault, entry) {
  const r = await getClient();
  if (!r) return;
  try { await r.hset(VAULT_REGISTRY_KEY, vault, JSON.stringify(entry)); } catch {}
}

async function loadVaultRegistry() {
  const r = await getClient();
  if (!r) return [];
  try {
    const h = await r.hgetall(VAULT_REGISTRY_KEY);
    return Object.entries(h)
      .map(([v, j]) => { try { return [v, JSON.parse(j)]; } catch { return null; } })
      .filter(Boolean);
  } catch { return []; }
}

// Drop pruned (long-dead) vaults from the persisted registry so the hash stays bounded.
async function removeVaultEntries(vaults) {
  if (!vaults || !vaults.length) return;
  const r = await getClient();
  if (!r) return;
  try { await r.hdel(VAULT_REGISTRY_KEY, ...vaults); } catch {}
}

module.exports = {
  writePrice, writePriceSol, writePool, writePoolByMint, writeMint,
  updateTrending, appendPriceHistory, writeCandleBuffer,
  getPrice, getPriceSol, getPool, getPoolByMint, getMint,
  getTrending, getPriceHistory, getCandles,
  saveVaultEntry, loadVaultRegistry, removeVaultEntries,
  writeNftListing, getNftListing, removeNftListing,
  writeNftListingsBatch, removeNftListingsBatch,
  cacheMintCollection, getCachedMintCollection, getCachedMintCollections, rebuildNftCollections, rebuildNftBids,
  cacheWhitelistVoc, getCachedWhitelistVocs,
  cacheCollName, getCachedCollNames,
  disconnect,
};
