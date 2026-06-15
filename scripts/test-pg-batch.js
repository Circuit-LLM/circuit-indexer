// scripts/test-pg-batch.js — verify the batched Postgres writer end-to-end.
//
// Exercises enqueue → timed/sized flush → readback, plus same-bucket coalescing,
// against the REAL circuit_index DB using fake TESTPGBATCH* keys that are cleaned up.
//
// Run: set -a; . ./.env; set +a; node scripts/test-pg-batch.js
'use strict';

const pgw = require('../writers/postgres');
const pg  = require('pg');

const TAG  = 'TESTPGBATCH';
const MINT = `${TAG}_${Date.now()}`;
const POOL = `${TAG}_POOL_${Date.now()}`;
const T0   = Date.now() - (Date.now() % 60000); // align to a 1m bucket

let failures = 0;
const ok   = (m) => console.log(`  ✅ ${m}`);
const bad  = (m) => { console.log(`  ❌ ${m}`); failures++; };

async function main() {
  const raw = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 1 });
  await raw.query('SELECT 1'); // confirm creds work

  // 1) enqueue should be synchronous + instant (no DB round-trip)
  const t = process.hrtime.bigint();
  pgw.writeToken({ mint: MINT, symbol: 'TST', name: 'Test', decimals: 6, tokenProgram: 'tok' });
  pgw.writePool({ poolAccount: POOL, type: 'test-amm', mint0: MINT, mint1: 'SOL', program: 'prog', slot: 1 });
  pgw.writeCandle({ mint: MINT, window: '1m', openTime: T0, open: 1, high: 2, low: 0.5, close: 1.5, volume: 10, ticks: 3, buys: 2, sells: 1 });
  // same bucket again → must coalesce (volume/ticks/buys/sells add, high=max, low=min)
  pgw.writeCandle({ mint: MINT, window: '1m', openTime: T0, open: 1, high: 5, low: 0.2, close: 9, volume: 4, ticks: 1, buys: 1, sells: 3 });
  const elapsedMs = Number(process.hrtime.bigint() - t) / 1e6;
  (elapsedMs < 5) ? ok(`enqueue is non-blocking (${elapsedMs.toFixed(2)}ms for 4 writes)`)
                  : bad(`enqueue too slow: ${elapsedMs.toFixed(2)}ms — did it hit the DB?`);

  const s = pgw.stats();
  (s.buffered >= 3) ? ok(`buffered in memory before flush (buffered=${s.buffered})`)
                    : bad(`expected buffered rows, got ${s.buffered}`);

  // 2) force the flush (disconnect drains, then re-open via read)
  await pgw.disconnect();

  // 3) read back from the REAL table via a fresh connection
  const tok = await raw.query('SELECT symbol, decimals FROM tokens WHERE mint=$1', [MINT]);
  (tok.rows[0]?.symbol === 'TST') ? ok('token flushed + readable') : bad(`token not found: ${JSON.stringify(tok.rows[0])}`);

  const pool = await raw.query('SELECT pool_type FROM pools WHERE pool_account=$1', [POOL]);
  (pool.rows[0]?.pool_type === 'test-amm') ? ok('pool flushed + readable') : bad(`pool not found: ${JSON.stringify(pool.rows[0])}`);

  const c = await raw.query('SELECT high, low, close, volume, ticks, buys, sells FROM ohlcv_candles WHERE mint=$1 AND tf=$2', [MINT, '1m']);
  const row = c.rows[0];
  if (!row) { bad('candle not found'); }
  else {
    const expect = { high: 5, low: 0.2, close: 9, volume: 14, ticks: 4, buys: 3, sells: 4 };
    const got = { high: +row.high, low: +row.low, close: +row.close, volume: +row.volume, ticks: row.ticks, buys: row.buys, sells: row.sells };
    const match = JSON.stringify(got) === JSON.stringify(expect);
    match ? ok(`candle coalesced correctly: ${JSON.stringify(got)}`)
          : bad(`candle merge wrong:\n      expected ${JSON.stringify(expect)}\n      got      ${JSON.stringify(got)}`);
  }

  const st = pgw.stats();
  (st.rowsFlushed >= 3 && st.flushErrors === 0) ? ok(`flush stats clean (flushed=${st.rowsFlushed}, errors=${st.flushErrors})`)
                                                 : bad(`flush stats: ${JSON.stringify(st)}`);

  // 4) cleanup
  await raw.query('DELETE FROM ohlcv_candles WHERE mint LIKE $1', [`${TAG}%`]);
  await raw.query('DELETE FROM pools  WHERE pool_account LIKE $1', [`${TAG}%`]);
  await raw.query('DELETE FROM tokens WHERE mint LIKE $1', [`${TAG}%`]);
  ok('test rows cleaned up');
  await raw.end();

  console.log(failures === 0 ? '\n✅ PHASE 2 BATCH WRITER: ALL CHECKS PASSED' : `\n❌ ${failures} CHECK(S) FAILED`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch(e => { console.error('test crashed:', e.message); process.exit(1); });
