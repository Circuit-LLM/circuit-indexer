// writers/redis.js — Write hot price data to Redis.
//
// Redis data structures:
//   circuit:price:{mint}          STRING  JSON USD price record, TTL 30s (stable-quoted pools only)
//   circuit:price-sol:{mint}      STRING  JSON SOL price record, TTL 120s (SOL-quoted pools)
//   circuit:pool:{poolAccount}    STRING  JSON pool state, TTL 60s
//   circuit:pool-by-mint:{mint}   STRING  poolAccount address, TTL 120s (reverse index)
//   circuit:mint:{mint}           STRING  JSON mint metadata, no TTL (stable)
//   circuit:trending              ZSET    score=accumulated volume (SOL), member=mint
//   circuit:ph:{mint}             LIST    price history ring buffer, max 300 entries, TTL 24h
//                                         each entry: JSON {p, ts} (priceSol, unix ms)
//   circuit:candles:1m:{mint}     LIST    1m OHLCV ring buffer, max 120 candles (~2h), TTL 4h
//   circuit:candles:5m:{mint}     LIST    5m OHLCV ring buffer, max 288 candles (~24h), TTL 36h
//   circuit:candles:1h:{mint}     LIST    1h OHLCV ring buffer, max 168 candles (~7d), TTL 8d
//   circuit:candles:1d:{mint}     LIST    1d OHLCV ring buffer, max 90 candles (~90d), TTL 92d
//                                         each entry: JSON {t, o, h, l, c, v, n, b, s}
//                                           t=openTime ms, o/h/l/c=OHLCV, n=ticks, b=buys, s=sells
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
  // Mint metadata is stable — no TTL
  await r.set(`circuit:mint:${mint}`, JSON.stringify({
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

module.exports = {
  writePrice, writePriceSol, writePool, writePoolByMint, writeMint,
  updateTrending, appendPriceHistory, writeCandleBuffer,
  getPrice, getPriceSol, getPool, getPoolByMint, getMint,
  getTrending, getPriceHistory, getCandles,
  disconnect,
};
