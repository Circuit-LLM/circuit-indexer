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
//
// HOT-PATH NOTE (Phase 2): writeToken/writePool/writeCandle do NOT touch the DB
// synchronously. They coalesce rows into in-memory buffers and return immediately;
// a background timer flushes batched multi-row upserts off the event path. Postgres
// is history/analytics only — the live trading path reads Redis — so a flush failure
// just drops a batch (logged) and never blocks or corrupts the indexer.
'use strict';

const Logger = require('../lib/logger');

const DB_URL = process.env.DATABASE_URL || 'postgresql://localhost/circuit_index';

// ── Batching config ─────────────────────────────────────────────────────────
const FLUSH_INTERVAL_MS = Number(process.env.PG_FLUSH_INTERVAL_MS || 2000);
const FLUSH_AT_ROWS     = Number(process.env.PG_FLUSH_AT_ROWS     || 1000); // size-trigger
const CHUNK_ROWS        = 1000;   // rows per INSERT statement (param-limit safe: 11*1000 < 65535)
const MAX_BUFFER_ROWS   = 50_000; // hard cap per buffer — drop-oldest if flush is failing/backed up

let _pool   = null;
let _failed = false; // don't retry after first connection failure

// Coalescing buffers (Map de-dupes within a flush window → also avoids the
// "ON CONFLICT cannot affect row a second time" error from duplicate keys in one INSERT).
const _candleBuf = new Map(); // `${openTime}|${mint}|${tf}` -> merged candle
const _poolBuf   = new Map(); // poolAccount -> row (first-seen wins, matches ON CONFLICT DO NOTHING)
const _tokenBuf  = new Map(); // mint -> row (last write wins)

let _flushing   = false;
let _inflight   = null;  // promise for the currently-running flush (so disconnect can await it)
let _flushTimer = null;

const _stats = {
  candlesBuffered: 0, poolsBuffered: 0, tokensBuffered: 0,
  rowsFlushed: 0, rowsDropped: 0, flushes: 0, flushErrors: 0, lastFlushMs: 0,
};

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
    Logger.info('PostgresWriter: connected', { url: DB_URL.replace(/\/\/[^@]*@/, '//***@') });
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

// ── Buffer helpers ──────────────────────────────────────────────────────────

// Bound a buffer: if it exceeds the hard cap (flush failing / DB down while we
// keep buffering), drop the oldest entries so memory stays bounded.
function _bound(buf) {
  while (buf.size > MAX_BUFFER_ROWS) {
    const oldest = buf.keys().next().value;
    buf.delete(oldest);
    _stats.rowsDropped++;
  }
}

function _maybeFlushSoon() {
  if (_candleBuf.size + _poolBuf.size + _tokenBuf.size >= FLUSH_AT_ROWS) {
    // size trigger — flush on next tick, don't block the caller
    setImmediate(() => { _flushAll().catch(() => {}); });
  }
}

// ── Write operations (enqueue only — return immediately) ──────────────────────

function writeToken(mintInfo) {
  if (_failed) return Promise.resolve();
  const { mint } = mintInfo;
  if (!mint) return Promise.resolve();
  _tokenBuf.set(mint, mintInfo); // last write wins
  _stats.tokensBuffered++;
  _bound(_tokenBuf);
  _maybeFlushSoon();
  return Promise.resolve();
}

function writePool(poolInfo) {
  if (_failed) return Promise.resolve();
  const { poolAccount } = poolInfo;
  if (!poolAccount) return Promise.resolve();
  if (!_poolBuf.has(poolAccount)) {       // first-seen wins (matches ON CONFLICT DO NOTHING)
    _poolBuf.set(poolAccount, poolInfo);
    _stats.poolsBuffered++;
    _bound(_poolBuf);
    _maybeFlushSoon();
  }
  return Promise.resolve();
}

function writeCandle(candle) {
  if (_failed) return Promise.resolve();
  const { mint, window: tf, openTime } = candle;
  if (!mint || !tf || openTime == null) return Promise.resolve();
  const key = `${openTime}|${mint}|${tf}`;
  const prev = _candleBuf.get(key);
  if (!prev) {
    _candleBuf.set(key, { ...candle });
  } else {
    // Coalesce same-bucket candles. Emissions are now cumulative snapshots of the open
    // bucket (not partials), so keep max/latest — summing would double-count volume.
    prev.high   = Math.max(prev.high, candle.high);
    prev.low    = Math.min(prev.low,  candle.low);
    prev.close  = candle.close;                       // latest close
    prev.volume = Math.max(prev.volume || 0, candle.volume || 0);
    prev.ticks  = Math.max(prev.ticks  || 0, candle.ticks  || 0);
    prev.buys   = Math.max(prev.buys   || 0, candle.buys   || 0);
    prev.sells  = Math.max(prev.sells  || 0, candle.sells  || 0);
  }
  _stats.candlesBuffered++;
  _bound(_candleBuf);
  _maybeFlushSoon();
  return Promise.resolve();
}

// ── Batched flush ─────────────────────────────────────────────────────────────

// Build a multi-row "INSERT ... VALUES (...),(...) ON CONFLICT ..." with positional params.
function _buildBatch(rows, cols, conflictClause) {
  const placeholders = [];
  const params = [];
  let i = 0;
  for (const row of rows) {
    const ph = [];
    for (const val of row) { params.push(val); ph.push(`$${++i}`); }
    placeholders.push(`(${ph.join(',')})`);
  }
  return { text: `INSERT INTO ${cols} VALUES ${placeholders.join(',')} ${conflictClause}`, params };
}

async function _flushTable(buf, mapRow, cols, conflictClause) {
  if (buf.size === 0) return;
  const p = await getPool();
  if (!p) return; // no-op mode — leave buffer; _bound caps it
  // Snapshot + clear so new writes during the flush land in the next window
  const entries = Array.from(buf.values());
  buf.clear();
  try {
    for (let off = 0; off < entries.length; off += CHUNK_ROWS) {
      const chunk = entries.slice(off, off + CHUNK_ROWS).map(mapRow);
      const { text, params } = _buildBatch(chunk, cols, conflictClause);
      await p.query(text, params);
      _stats.rowsFlushed += chunk.length;
    }
  } catch (e) {
    _stats.flushErrors++;
    _stats.rowsDropped += entries.length; // dropped: PG is non-critical history, never block the indexer
    Logger.error('PostgresWriter: flush failed (batch dropped)', { table: cols.split(' ')[0], rows: entries.length, error: e.message });
  }
}

function _flushAll() {
  if (_flushing) return _inflight || Promise.resolve(); // let callers await the in-flight flush
  _flushing = true;
  _inflight = _runFlush();
  return _inflight;
}

async function _runFlush() {
  const t0 = Date.now();
  try {
    await _flushTable(
      _tokenBuf,
      (m) => [m.mint, m.symbol || null, m.name || null, m.decimals ?? null, m.tokenProgram || null,
              m.mintAuthority || null, m.freezeAuthority || null, m.supply || null, m.slot || null],
      `tokens (mint, symbol, name, decimals, token_program, mint_authority, freeze_authority, supply, first_seen_slot)`,
      `ON CONFLICT (mint) DO UPDATE SET
         symbol=EXCLUDED.symbol, name=EXCLUDED.name,
         mint_authority=EXCLUDED.mint_authority,
         freeze_authority=EXCLUDED.freeze_authority,
         supply=EXCLUDED.supply, updated_at=NOW()`
    );
    await _flushTable(
      _poolBuf,
      (pi) => [pi.poolAccount, pi.type, pi.mintA || pi.mint0, pi.mintB || pi.mint1,
               pi.program || null, pi.feeRate || null, pi.tickSpacing || null, pi.slot || null],
      `pools (pool_account, pool_type, mint_a, mint_b, program, fee_rate, tick_spacing, first_seen_slot)`,
      `ON CONFLICT (pool_account) DO NOTHING`
    );
    await _flushTable(
      _candleBuf,
      // openTime is epoch-ms; pass a Date so node-postgres serializes it to timestamptz
      // (avoids needing to_timestamp() in SQL, which the generic batch builder can't emit).
      (c) => [new Date(c.openTime), c.mint, c.window, c.open, c.high, c.low, c.close, c.volume, c.ticks, c.buys ?? 0, c.sells ?? 0],
      `ohlcv_candles (time, mint, tf, open, high, low, close, volume, ticks, buys, sells)`,
      // Cumulative-snapshot upsert: a bucket re-emitted across flushes is a full snapshot,
      // not an increment, so keep max/latest (summing would double-count volume/ticks).
      `ON CONFLICT (time, mint, tf) DO UPDATE SET
         high   = GREATEST(ohlcv_candles.high,   EXCLUDED.high),
         low    = LEAST(ohlcv_candles.low,       EXCLUDED.low),
         close  = EXCLUDED.close,
         volume = GREATEST(ohlcv_candles.volume, EXCLUDED.volume),
         ticks  = GREATEST(ohlcv_candles.ticks,  EXCLUDED.ticks),
         buys   = GREATEST(ohlcv_candles.buys,   EXCLUDED.buys),
         sells  = GREATEST(ohlcv_candles.sells,  EXCLUDED.sells)`
    );
    _stats.flushes++;
    _stats.lastFlushMs = Date.now() - t0;
  } finally {
    _flushing = false;
  }
}

function _startFlushTimer() {
  if (_flushTimer) return;
  _flushTimer = setInterval(() => { _flushAll().catch(() => {}); }, FLUSH_INTERVAL_MS);
  _flushTimer.unref();
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

function stats() { return { ..._stats, buffered: _candleBuf.size + _poolBuf.size + _tokenBuf.size }; }

async function disconnect() {
  if (_flushTimer) { clearInterval(_flushTimer); _flushTimer = null; }
  await _flushAll().catch(() => {}); // finish any in-flight flush (returns the in-flight promise)
  await _flushAll().catch(() => {}); // drain rows buffered during that flush
  if (_pool) { await _pool.end(); _pool = null; }
}

_startFlushTimer();

module.exports = { writeToken, writePool, writeCandle, getCandles, getToken, disconnect, stats };
