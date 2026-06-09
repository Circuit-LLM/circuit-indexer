// writers/postgres.js — Write OHLCV candles and token metadata to Postgres.
//
// Uses TimescaleDB extension for time-series hypertables on ohlcv_candles.
// Falls back to regular Postgres if TimescaleDB is not installed.
//
// Schema:
//   tokens (mint, symbol, name, decimals, token_program, first_seen_slot, first_seen_at)
//   pools  (pool_account, type, mint_a, mint_b, program, first_seen_slot, first_seen_at)
//   ohlcv_candles (time, mint, window, open, high, low, close, volume, ticks)
//
// Requires: npm install pg && sudo apt-get install -y postgresql
// This module is a no-op if Postgres is not available.
'use strict';

const Logger = require('../lib/logger');

const DB_URL = process.env.DATABASE_URL || 'postgresql://localhost/circuit_index';

let _pool = null;
let _failed = false; // don't retry after first connection failure

async function getPool() {
  if (_pool)   return _pool;
  if (_failed) return null;
  let pg;
  try {
    pg = require('pg');
  } catch {
    Logger.warn('PostgresWriter: pg not installed — running in no-op mode. npm install pg');
    _failed = true;
    return null;
  }
  _pool = new pg.Pool({ connectionString: DB_URL });
  _pool.on('error', (e) => Logger.error('PostgresWriter: pool error', { error: e.message }));
  try {
    await _pool.query('SELECT 1');
    await _ensureSchema();
    Logger.info('PostgresWriter: connected', { url: DB_URL });
  } catch (e) {
    Logger.warn('PostgresWriter: could not connect — no-op mode', { error: e.message });
    _pool  = null;
    _failed = true;
  }
  return _pool;
}

async function _ensureSchema() {
  const client = await _pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS tokens (
        mint            TEXT PRIMARY KEY,
        symbol          TEXT,
        name            TEXT,
        decimals        SMALLINT,
        token_program   TEXT,
        mint_authority  TEXT,
        freeze_authority TEXT,
        supply          NUMERIC,
        first_seen_slot BIGINT,
        first_seen_at   TIMESTAMPTZ DEFAULT NOW(),
        updated_at      TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS pools (
        pool_account    TEXT PRIMARY KEY,
        pool_type       TEXT,
        mint_a          TEXT,
        mint_b          TEXT,
        program         TEXT,
        fee_rate        NUMERIC,
        tick_spacing    INT,
        first_seen_slot BIGINT,
        first_seen_at   TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS ohlcv_candles (
        time        TIMESTAMPTZ NOT NULL,
        mint        TEXT        NOT NULL,
        window      TEXT        NOT NULL,
        open        NUMERIC,
        high        NUMERIC,
        low         NUMERIC,
        close       NUMERIC,
        volume      NUMERIC,
        ticks       INT,
        PRIMARY KEY (time, mint, window)
      );
    `);

    // Try to create TimescaleDB hypertable (silently skip if extension not available)
    try {
      await client.query(`
        SELECT create_hypertable('ohlcv_candles', 'time', if_not_exists => TRUE);
      `);
      Logger.info('PostgresWriter: TimescaleDB hypertable configured');
    } catch {
      Logger.info('PostgresWriter: TimescaleDB not available — using regular table');
    }

    // Index for fast token queries
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_ohlcv_mint_window ON ohlcv_candles (mint, window, time DESC);
      CREATE INDEX IF NOT EXISTS idx_pools_mints ON pools (mint_a, mint_b);
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
  `, [mint, symbol || null, name || null, decimals || null, tokenProgram || null,
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
  const { mint, window, openTime, open, high, low, close, volume, ticks } = candle;
  await p.query(`
    INSERT INTO ohlcv_candles (time, mint, window, open, high, low, close, volume, ticks)
    VALUES (to_timestamp($1 / 1000.0), $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (time, mint, window) DO UPDATE SET
      high=GREATEST(ohlcv_candles.high, EXCLUDED.high),
      low=LEAST(ohlcv_candles.low, EXCLUDED.low),
      close=EXCLUDED.close,
      volume=ohlcv_candles.volume + EXCLUDED.volume,
      ticks=ohlcv_candles.ticks + EXCLUDED.ticks
  `, [openTime, mint, window, open, high, low, close, volume, ticks]);
}

// ── Read operations ───────────────────────────────────────────────────────────

async function getCandles(mint, windowName, limit = 100) {
  const p = await getPool();
  if (!p) return [];
  const { rows } = await p.query(`
    SELECT extract(epoch from time)*1000 AS ts, open, high, low, close, volume, ticks
    FROM ohlcv_candles
    WHERE mint=$1 AND window=$2
    ORDER BY time DESC LIMIT $3
  `, [mint, windowName, limit]);
  return rows.reverse(); // chronological order
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
