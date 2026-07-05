#!/usr/bin/env node
// parity-snapshot.js — READ-ONLY downstream health snapshot, for before/after a subscription change.
// Signals that catch "we ruined downstream":
//   • per-pool-type liveness: fresh circuit:price-sol writes grouped by `source` (raydium-clmm, orca-whirlpool,
//     raydium-cpmm, pumpswap, pumpfun, …). If a type's fresh-count collapses to ~0, that pool type stopped pricing.
//   • key-space counts (price / price-sol / pool) + trending size — should not collapse.
//   • end-to-end: hit the live price-feed (:18941) /price/:mint for one fresh mint per source → 200 + price.
// Usage: node tools/parity-snapshot.js <label>   →   prints one JSON line (also appended to tools/parity-log.jsonl)
'use strict';
(function loadEnv() {
  const fs = require('fs'), path = require('path');
  try { for (const l of fs.readFileSync(path.join(__dirname, '..', '.env'), 'utf8').split('\n')) {
    const s = l.trim(); if (!s || s.startsWith('#')) continue; const i = s.indexOf('='); if (i < 0) continue;
    const k = s.slice(0, i).trim(); let v = s.slice(i + 1).trim();
    if ((v[0] === '"' && v.endsWith('"')) || (v[0] === "'" && v.endsWith("'"))) v = v.slice(1, -1);
    if (!(k in process.env)) process.env[k] = v;
  } } catch {}
})();

const FRESH_MS = 90_000;             // "fresh" = written within 90s
const FEED = 'http://127.0.0.1:18941';

async function scanCount(r, pattern, cap = 200000) {
  let cur = '0', n = 0;
  do { const [nx, ks] = await r.scan(cur, 'MATCH', pattern, 'COUNT', 5000); cur = nx; n += ks.length; if (n >= cap) break; } while (cur !== '0');
  return n;
}

(async () => {
  const label = process.argv[2] || 'snapshot';
  const now = Date.now();
  const Redis = require('ioredis');
  const r = new Redis(process.env.REDIS_URL, { lazyConnect: true, maxRetriesPerRequest: 2 });
  await r.connect();

  // price-sol by source, with freshness + a representative fresh mint per source
  const bySrc = {};                  // source → { total, fresh, mint }
  let cur = '0', scanned = 0;
  do {
    const [nx, keys] = await r.scan(cur, 'MATCH', 'circuit:price-sol:*', 'COUNT', 2000);
    cur = nx;
    if (keys.length) {
      const vals = await r.mget(keys);
      keys.forEach((k, i) => {
        if (!vals[i]) return;
        let o; try { o = JSON.parse(vals[i]); } catch { return; }
        const s = o.source || o.src || 'unknown';
        const b = (bySrc[s] ||= { total: 0, fresh: 0, mint: null });
        b.total++;
        const ts = Number(o.ts || o.t || 0);
        if (ts && now - ts <= FRESH_MS) { b.fresh++; if (!b.mint) b.mint = k.slice('circuit:price-sol:'.length); }
      });
    }
    scanned += keys.length;
  } while (cur !== '0' && scanned < 200000);

  // key-space counts + trending
  const counts = {
    price:    await scanCount(r, 'circuit:price:*'),
    priceSol: scanned,
    pool:     await scanCount(r, 'circuit:pool:*'),
    trending: await r.zcard('circuit:trending').catch(() => 0),
  };
  await r.quit();

  // end-to-end price-feed check: one fresh mint per source
  const feed = {};
  for (const [s, b] of Object.entries(bySrc)) {
    if (!b.mint) { feed[s] = 'no-fresh-mint'; continue; }
    try {
      const resp = await fetch(`${FEED}/price/${b.mint}`, { signal: AbortSignal.timeout(5000) });
      const j = await resp.json().catch(() => ({}));
      const price = j.price ?? j.priceUsd ?? j.priceSol ?? j.data?.price;
      feed[s] = `${resp.status}${price != null && price > 0 ? ' ok(' + Number(price).toPrecision(4) + ')' : ' NO-PRICE'}`;
    } catch (e) { feed[s] = 'ERR:' + e.message; }
  }

  const snap = { label, at: new Date(now).toISOString(),
    sources: Object.fromEntries(Object.entries(bySrc).sort((a, b) => b[1].total - a[1].total)
      .map(([s, b]) => [s, { total: b.total, fresh: b.fresh }])),
    counts, feed };
  const line = JSON.stringify(snap);
  console.log(line);
  try { require('fs').appendFileSync(require('path').join(__dirname, 'parity-log.jsonl'), line + '\n'); } catch {}
})().catch((e) => { console.error('parity-snapshot error:', e); process.exit(1); });
