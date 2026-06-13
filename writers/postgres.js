// writers/postgres.js — Write OHLCV candles and token metadata to Postgres.
//
// Schema (plain Postgres, no TimescaleDB required):
//   tokens       (mint, symbol, name, decimals, token_program, ...)
//   pools        (pool_account, pool_type, mint_a, mint_b, ...)
//   ohlcv_candles(time, mint, tf, open, high, low, close, volume, ticks, buys, sells)
//
// Column "tf" (timeframe) replaces "window" which is a PG16 reserved word.
// Data retention is handled externally by a systemd timer (circuit-candle-retention.service).
//
// No-op if Postgres is unavailable — indexer runs fine without it.
'use strict';

const Logger = require('../lib/logger');

const DB_URL = process.env.DATABASE_URL || 'postgresql://localhost/circuit_index';

let _pool   = null;
let _failed = false; // don't retry after first connection failure

async function getPool() {
  if (_pool)   return _pool;
  if (_failed) return null;
  let pg;
  try {
    pg = require('pg');
  } catch {
    Logger.warn('PostgresWriter: pg not installed — no-op mode');
    _failed = true;
    return null;
  }
  _pool = new pg.Pool({ connectionString: DB_URL, max: 5 });
  _pool.on('error', (e) => Logger.error('PostgresWriter: pool error', { error: e.message }));
  try {
    await _pool.query('SELECT 1');
    await _ensureSchema();
    Logger.info('PostgresWriter: connected', { url: DB_URL });
  } catch (e) {
    Logger.warn('PostgresWriter: could not connect — no-op mode', { error: e.message });
    _pool   = null;
    _failed = true;
  }
  return _pool;
}

async function _ensureSchema() {
  const client = await _pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS tokens (
        mint              TEXT PRIMARY KEY,
        symbol            TEXT,
        name              TEXT,
        decimals          SMALLINT,
        token_program     TEXT,
        mint_authority    TEXT,
        freeze_authority  TEXT,
        supply            NUMERIC,
        first_seen_slot   BIGINT,
        first_seen_at     TIMESTAMPTZ DEFAULT NOW(),
        updated_at        TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS pools (
        pool_account      TEXT PRIMARY KEY,
        pool_type         TEXT,
        mint_a            TEXT,
        mint_b            TEXT,
        program           TEXT,
        fee_rate          NUMERIC,
        tick_spacing      INT,
        first_seen_slot   BIGINT,
        first_seen_at     TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS ohlcv_candles (
        time    TIMESTAMPTZ      NOT NULL,
        mint    TEXT             NOT NULL,
        tf      TEXT             NOT NULL,
        open    DOUBLE PRECISION,
        high    DOUBLE PRECISION,
        low     DOUBLE PRECISION,
        close   DOUBLE PRECISION,
        volume  DOUBLE PRECISION,
        ticks   INT,
        buys    INT DEFAULT 0,
        sells   INT DEFAULT 0,
        PRIMARY KEY (time, mint, tf)
      );

      CREATE INDEX IF NOT EXISTS idx_ohlcv_mint_tf ON ohlcv_candles (mint, tf, time DESC);
      CREATE INDEX IF NOT EXISTS idx_pools_mints   ON pools (mint_a, mint_b);
    `);
  } finally {
    client.release();
  }
}

// ── Write operations ──────────────────────────────────────────────────────────

async function writeToken(mintInfo) {
  const p = await getPool();
  if (!p) return;
  const { mint, symbol, name, decimals, tokenProgram, mintAuthority, freezeAuthority, supply, slot } = mintInfo;
  await p.query(`
    INSERT INTO tokens (mint, symbol, name, decimals, token_program, mint_authority, freeze_authority, supply, first_seen_slot)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (mint) DO UPDATE SET
      symbol=EXCLUDED.symbol, name=EXCLUDED.name,
      mint_authority=EXCLUDED.mint_authority,
      freeze_authority=EXCLUDED.freeze_authority,
      supply=EXCLUDED.supply,
      updated_at=NOW()
  `, [mint, symbol || null, name || null, decimals ?? null, tokenProgram || null,
      mintAuthority || null, freezeAuthority || null, supply || null, slot || null]);
}

async function writePool(poolInfo) {
  const p = await getPool();
  if (!p) return;
  const { poolAccount, type, mint0, mint1, mintA, mintB, program, feeRate, tickSpacing, slot } = poolInfo;
  const a = mintA || mint0;
  const b = mintB || mint1;
  await p.query(`
    INSERT INTO pools (pool_account, pool_type, mint_a, mint_b, program, fee_rate, tick_spacing, first_seen_slot)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (pool_account) DO NOTHING
  `, [poolAccount, type, a, b, program || null, feeRate || null, tickSpacing || null, slot || null]);
}

async function writeCandle(candle) {
  const p = await getPool();
  if (!p) return;
  const { mint, window: tf, openTime, open, high, low, close, volume, ticks, buys = 0, sells = 0 } = candle;
  await p.query(`
    INSERT INTO ohlcv_candles (time, mint, tf, open, high, low, close, volume, ticks, buys, sells)
    VALUES (to_timestamp($1 / 1000.0), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT (time, mint, tf) DO UPDATE SET
      high   = GREATEST(ohlcv_candles.high,  EXCLUDED.high),
      low    = LEAST(ohlcv_candles.low,       EXCLUDED.low),
      close  = EXCLUDED.close,
      volume = ohlcv_candles.volume + EXCLUDED.volume,
      ticks  = ohlcv_candles.ticks  + EXCLUDED.ticks,
      buys   = ohlcv_candles.buys   + EXCLUDED.buys,
      sells  = ohlcv_candles.sells  + EXCLUDED.sells
  `, [openTime, mint, tf, open, high, low, close, volume, ticks, buys, sells]);
}

// ── Read operations ───────────────────────────────────────────────────────────

async function getCandles(mint, tf, limit = 200) {
  const p = await getPool();
  if (!p) return [];
  const { rows } = await p.query(`
    SELECT extract(epoch from time)*1000 AS t, open AS o, high AS h, low AS l, close AS c,
           volume AS v, ticks AS n, buys AS b, sells AS s
    FROM ohlcv_candles
    WHERE mint=$1 AND tf=$2
    ORDER BY time DESC LIMIT $3
  `, [mint, tf, limit]);
  return rows.reverse(); // chronological
}

async function getToken(mint) {
  const p = await getPool();
  if (!p) return null;
  const { rows } = await p.query('SELECT * FROM tokens WHERE mint=$1', [mint]);
  return rows[0] || null;
}

async function disconnect() {
  if (_pool) { await _pool.end(); _pool = null; }
}

module.exports = { writeToken, writePool, writeCandle, getCandles, getToken, disconnect };
