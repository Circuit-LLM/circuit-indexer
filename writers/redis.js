// writers/redis.js — Write hot price data to Redis.
//
// Three Redis data structures:
//   circuit:price:{mint}        STRING  JSON price record, TTL 30s
//   circuit:pool:{poolAccount}  STRING  JSON pool state, TTL 60s
//   circuit:mint:{mint}         STRING  JSON mint metadata, no TTL (stable)
//   circuit:trending          ZSET    score=volumeUsd5m, member=mint
//
// Requires Redis ≥ 6.2. Install: sudo apt-get install -y redis-server
// This module is a no-op if Redis is not available.
'use strict';

const Logger = require('../lib/logger');

const PRICE_TTL   = 30;  // seconds
const POOL_TTL    = 60;
const TRENDING_WINDOW = 5 * 60 * 1000; // 5 minutes

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
    Logger.info('RedisWriter: connected', { url });
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

async function updateTrending(mint, volumeUsdDelta) {
  const r = await getClient();
  if (!r) return;
  // ZINCRBY — increment score (5m volume) for this mint
  await r.zincrby('circuit:trending', volumeUsdDelta, mint);
}

// ── Read operations (for circuit-data-api onchain drivers) ─────────────────────

async function getPrice(mint) {
  const r = await getClient();
  if (!r) return null;
  const raw = await r.get(`circuit:price:${mint}`);
  return raw ? JSON.parse(raw) : null;
}

async function getPool(poolAccount) {
  const r = await getClient();
  if (!r) return null;
  const raw = await r.get(`circuit:pool:${poolAccount}`);
  return raw ? JSON.parse(raw) : null;
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
  // Highest volume first
  const raw = await r.zrevrange('circuit:trending', 0, limit - 1, 'WITHSCORES');
  const out = [];
  for (let i = 0; i < raw.length; i += 2) {
    out.push({ mint: raw[i], volumeUsd5m: parseFloat(raw[i + 1]) });
  }
  return out;
}

async function disconnect() {
  if (_client) { await _client.quit(); _client = null; }
}

module.exports = {
  writePrice, writePool, writeMint, updateTrending,
  getPrice, getPool, getMint, getTrending,
  disconnect,
};
