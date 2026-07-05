#!/usr/bin/env node
// verify-slice.js — READ-ONLY proof that streaming only a prefix of each account
// (via Triton accountsDataSlice) is lossless for every parser.
//
// For a sample of REAL live accounts (pool addresses from circuit:pool:*, vaults from
// circuit:vault-registry, mints from circuit:mint:*), it compares, per account:
//   baseline  = parse(full real bytes)
//   zeroed    = parse(full real bytes with everything at/after the slice zeroed)  [same length]
//   truncated = parse(real bytes cut to the slice length)                          [real post-slice length]
//
// zeroed == baseline  ⇒ the parser's output depends ONLY on bytes [0, slice). (the core proof)
// truncated behaviour ⇒ shows which length-guards must be relaxed (guard > slice ⇒ dropped).
//
// Nothing is written; the running indexer is untouched. Usage: node tools/verify-slice.js
'use strict';
// Minimal .env loader (dotenv isn't a dep here; the service gets env from systemd).
// Must run BEFORE requiring lib/rpc-client, which binds RPC_URL at module-load time.
(function loadEnv() {
  const fs = require('fs'), path = require('path');
  try {
    for (const line of fs.readFileSync(path.join(__dirname, '..', '.env'), 'utf8').split('\n')) {
      const s = line.trim();
      if (!s || s.startsWith('#')) continue;
      const i = s.indexOf('=');
      if (i < 0) continue;
      const k = s.slice(0, i).trim();
      let v = s.slice(i + 1).trim();
      if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
      if (!(k in process.env)) process.env[k] = v;
    }
  } catch (e) { console.error('could not read .env:', e.message); }
})();

const { rpcCall } = require('../lib/rpc-client');
const raydium  = require('../parsers/raydium');
const orca     = require('../parsers/orca');
const cpmm     = require('../parsers/cpmm');
const pumpswap = require('../parsers/pumpswap');
const pumpfun  = require('../parsers/pumpfun');
const token    = require('../parsers/token');

const SLICE_A = 333;                 // Move A: single global slice length (tallest requirement = CPMM 333)
const SLICE_B = { pumpfun: 72, vault: 72 }; // Move B: tighter second-stream slice for the small/frequent accounts
// TARGET = the per-tier slice each account type would actually get in the 3-stream design.
// Stream A(72): vault, pumpfun · Stream B(215): pumpswap, orca · Stream C(333): clmm, cpmm, mint(82).
const TARGET = { 'raydium-clmm': 333, 'raydium-cpmm': 333, orca: 215, pumpswap: 215, pumpfun: 72, vault: 72, mint: 82 };

// owner → { type, parse(event), guard, moveBslice? }
const CLMM   = 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK';
const ORCA   = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc';
const PSWAP  = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
const RCPMM  = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C';
const PUMP   = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const TOK    = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const TOK22  = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';

// Vault balance read is inline in indexer.js (not a parser export) — replicate it exactly.
const parseVault = (ev) => {
  const b = Buffer.isBuffer(ev.data) ? ev.data : Buffer.from(ev.data);
  return b.length >= 72 ? { balance: b.readBigUInt64LE(64) } : null;
};

function classify(owner, len) {
  if (owner === CLMM)  return { type: 'raydium-clmm', parse: raydium.processAccountEvent,  guard: 1544 };
  if (owner === ORCA)  return { type: 'orca',         parse: orca.processAccountEvent,     guard: 653  };
  if (owner === PSWAP) return { type: 'pumpswap',     parse: pumpswap.processAccountEvent, guard: 301  };
  if (owner === RCPMM) return { type: 'raydium-cpmm', parse: cpmm.processAccountEvent,     guard: 333  };
  if (owner === PUMP)  return { type: 'pumpfun',      parse: pumpfun.processAccountEvent,  guard: 49, moveB: SLICE_B.pumpfun };
  if (owner === TOK || owner === TOK22) {
    if (len === 82)  return { type: 'mint',  parse: (ev) => token.parseMint(Buffer.isBuffer(ev.data) ? ev.data : Buffer.from(ev.data), ev.owner), guard: 82 };
    if (len === 165) return { type: 'vault', parse: parseVault, guard: 72, moveB: SLICE_B.vault };
  }
  return null;
}

const canon = (o) => JSON.stringify(o, (_k, v) => (typeof v === 'bigint' ? 'n:' + v.toString() : v));

// same length, bytes [slice..] zeroed → proves output depends only on [0,slice)
function zeroTail(buf, slice) { const b = Buffer.from(buf); if (slice < b.length) b.fill(0, slice); return b; }
// real post-slice length
function truncate(buf, slice) { return buf.subarray(0, Math.min(slice, buf.length)); }

async function getMany(addrs) {
  const out = [];
  for (let i = 0; i < addrs.length; i += 100) {
    const batch = addrs.slice(i, i + 100);
    const r = await rpcCall('getMultipleAccounts', [batch, { encoding: 'base64' }]);
    const vals = r?.value || [];
    vals.forEach((v, j) => {
      if (v?.data?.[0]) out.push({ pubkey: batch[j], owner: v.owner, data: Buffer.from(v.data[0], 'base64') });
    });
  }
  return out;
}

async function scanKeys(redis, pattern, cap) {
  const keys = [];
  let cursor = '0';
  do {
    const [next, batch] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 1000);
    cursor = next;
    for (const k of batch) { keys.push(k); if (keys.length >= cap) return keys; }
  } while (cursor !== '0');
  return keys;
}
const sample = (arr, n) => (arr.length <= n ? arr : arr.filter((_, i) => i % Math.ceil(arr.length / n) === 0).slice(0, n));

(async () => {
  const Redis = require('ioredis');
  const redis = new Redis(process.env.REDIS_URL || 'redis://127.0.0.1:6379', { lazyConnect: true, maxRetriesPerRequest: 2 });
  await redis.connect();

  process.stdout.write('gathering live addresses from Redis…\n');
  const poolKeys  = await scanKeys(redis, 'circuit:pool:*', 8000);
  const mintKeys  = await scanKeys(redis, 'circuit:mint:*', 3000);
  const vaultMap  = await redis.hgetall('circuit:vault-registry').catch(() => ({}));
  await redis.quit();

  const poolAddrs  = sample(poolKeys.map((k) => k.slice('circuit:pool:'.length)), 2500);
  const mintAddrs  = sample(mintKeys.map((k) => k.slice('circuit:mint:'.length)), 400);
  const vaultAddrs = sample(Object.keys(vaultMap || {}), 500);
  process.stdout.write(`fetching ${poolAddrs.length} pools, ${vaultAddrs.length} vaults, ${mintAddrs.length} mints via RPC…\n`);

  const accts = [
    ...(await getMany(poolAddrs)),
    ...(await getMany(vaultAddrs)),
    ...(await getMany(mintAddrs)),
  ];

  // type → tally
  const T = {};
  const bump = (t) => (T[t] ||= { tested: 0, baseNull: 0, zeroMismatch: 0, truncParsedOk: 0, truncBlocked: 0, truncMismatch: 0, moveBmismatch: 0, moveBtested: 0, tgtTested: 0, tgtMismatch: 0, tgtTruncOk: 0, tgtTruncBlocked: 0, tgtTruncMismatch: 0, mismatches: [] });

  for (const a of accts) {
    const c = classify(a.owner, a.data.length);
    if (!c) continue;
    const t = bump(c.type);
    t.tested++;
    const ev = { type: 'account', owner: a.owner, pubkey: a.pubkey, slot: 0, ts: 0, data: a.data };
    const base = c.parse(ev);
    if (base == null) { t.baseNull++; continue; }         // parser itself returned null on full bytes — not our concern
    const baseC = canon(base);

    // Move A — zeroed tail (same length): the core losslessness proof
    const zc = canon(c.parse({ ...ev, data: zeroTail(a.data, SLICE_A) }));
    if (zc !== baseC) { t.zeroMismatch++; if (t.mismatches.length < 3) t.mismatches.push({ pk: a.pubkey, test: 'zeroTail@333' }); }

    // Move A — truncated length: shows real post-slice guard behaviour
    const tr = c.parse({ ...ev, data: truncate(a.data, SLICE_A) });
    if (tr == null) { if (c.guard > SLICE_A) t.truncBlocked++; else { t.truncMismatch++; if (t.mismatches.length < 3) t.mismatches.push({ pk: a.pubkey, test: 'trunc@333→null' }); } }
    else { canon(tr) === baseC ? t.truncParsedOk++ : (t.truncMismatch++, t.mismatches.length < 3 && t.mismatches.push({ pk: a.pubkey, test: 'trunc@333≠base' })); }

    // Move B — tighter slice for the small/frequent accounts (pumpfun, vault)
    if (c.moveB) {
      t.moveBtested++;
      const bc = canon(c.parse({ ...ev, data: zeroTail(a.data, c.moveB) }));
      if (bc !== baseC) { t.moveBmismatch++; if (t.mismatches.length < 3) t.mismatches.push({ pk: a.pubkey, test: `zeroTail@${c.moveB}` }); }
    }

    // Per-tier TARGET slice — the exact slice this type gets in the 3-stream design
    const tgt = TARGET[c.type];
    if (tgt != null) {
      t.tgtTested++;
      const gc = canon(c.parse({ ...ev, data: zeroTail(a.data, tgt) }));
      if (gc !== baseC) { t.tgtMismatch++; if (t.mismatches.length < 3) t.mismatches.push({ pk: a.pubkey, test: `zeroTail@target${tgt}` }); }
      // truncated to the design slice — exercises the parser length GUARD at the real post-slice length
      const tt = c.parse({ ...ev, data: truncate(a.data, tgt) });
      if (tt == null) { t.tgtTruncBlocked++; if (t.mismatches.length < 3) t.mismatches.push({ pk: a.pubkey, test: `trunc@target${tgt}→null(GUARD)` }); }
      else if (canon(tt) !== baseC) { t.tgtTruncMismatch++; if (t.mismatches.length < 3) t.mismatches.push({ pk: a.pubkey, test: `trunc@target${tgt}≠base` }); }
      else t.tgtTruncOk++;
    }
  }

  // ── report ──
  console.log('\n================ SLICE SAFETY PROOF ================');
  let anyFail = false;
  for (const [type, s] of Object.entries(T)) {
    const guardNote = ({ 'raydium-clmm': '1544→273', orca: '653→213' })[type];
    const pass = s.zeroMismatch === 0 && s.truncMismatch === 0 && s.moveBmismatch === 0 && s.tgtMismatch === 0 && s.tgtTruncBlocked === 0 && s.tgtTruncMismatch === 0;
    if (!pass) anyFail = true;
    console.log(`\n${type.toUpperCase()}  (${s.tested} live accounts, ${s.baseNull} parsed-null on full — skipped)`);
    console.log(`  DESIGN  zeroed-tail@target(${TARGET[type]}) identical  : ${s.tgtTested - s.tgtMismatch}/${s.tgtTested}  ${s.tgtMismatch ? '❌ ' + s.tgtMismatch + ' MISMATCH' : '✅'}`);
    console.log(`  DESIGN  truncated@target(${TARGET[type]}) parses same : ${s.tgtTruncOk}/${s.tgtTested}  ${(s.tgtTruncBlocked||s.tgtTruncMismatch) ? '❌ blocked ' + s.tgtTruncBlocked + ' mismatch ' + s.tgtTruncMismatch + ' (GUARD too high)' : '✅'}`);
    console.log(`  Move A  zeroed-tail@333 identical : ${s.tested - s.baseNull - s.zeroMismatch}/${s.tested - s.baseNull}  ${s.zeroMismatch ? '❌ ' + s.zeroMismatch + ' MISMATCH' : '✅'}`);
    console.log(`  Move A  truncated@333             : parsed-identical ${s.truncParsedOk}, guard-blocked(needs relax ${guardNote || '—'}) ${s.truncBlocked}${s.truncMismatch ? ', ❌ mismatch ' + s.truncMismatch : ''}`);
    if (s.moveBtested) console.log(`  Move B  zeroed-tail@72 identical  : ${s.moveBtested - s.moveBmismatch}/${s.moveBtested}  ${s.moveBmismatch ? '❌ ' + s.moveBmismatch + ' MISMATCH' : '✅'}`);
    if (s.mismatches.length) console.log('  ⚠ samples:', JSON.stringify(s.mismatches));
  }
  console.log('\n====================================================');
  console.log(anyFail
    ? '❌ FAIL — at least one parser depends on bytes beyond the slice. Do NOT slice.'
    : '✅ PASS — every parser output depends only on its slice prefix. Slicing is lossless (with CLMM 1544→273, Orca 653→213 guard relaxations).');
  console.log('   truncated@333 "guard-blocked" for CLMM/Orca is EXPECTED — it is exactly why those two guards must be relaxed.');
  process.exit(anyFail ? 1 : 0);
})().catch((e) => { console.error('verify-slice error:', e); process.exit(2); });
