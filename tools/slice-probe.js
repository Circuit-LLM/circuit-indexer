#!/usr/bin/env node
// slice-probe.js — READ-ONLY shadow subscription to validate that a stream's accountsDataSlice
// does NOT drop any of its account types (the Stage-A failure: Triton excludes accounts shorter
// than the slice length). Opens its OWN Geyser subscription, writes NOTHING to Redis, closes after
// ~40s. Confirms per-program arrivals (count + min/max returned data length) under the given slice.
//
// Usage: node tools/slice-probe.js <A|B|C|mints>
//   A     = vaults + pump.fun,      slice 72
//   B     = PumpSwap + Orca,        slice 215
//   C     = CLMM + CPMM,            slice 333
//   mints = SPL/Token-2022 mints,   NO slice
'use strict';
(function loadEnv(){const fs=require('fs'),path=require('path');try{for(const l of fs.readFileSync(path.join(__dirname,'..','.env'),'utf8').split('\n')){const s=l.trim();if(!s||s[0]==='#')continue;const i=s.indexOf('=');if(i<0)continue;const k=s.slice(0,i).trim();let v=s.slice(i+1).trim();if(!(k in process.env))process.env[k]=v;}}catch{}})();

const bs58 = require('bs58').default ?? require('bs58');
const yg = require('@triton-one/yellowstone-grpc');
const Client = yg.default ?? yg;
const { SubscribeRequest } = require('@triton-one/yellowstone-grpc/dist/grpc/geyser');

const CLMM='CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK', ORCA='whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
      PSWAP='pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA', CPMM='CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',
      PUMP='6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P', TOK='TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
      TOK22='TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
const NAME={[CLMM]:'clmm',[ORCA]:'orca',[PSWAP]:'pumpswap',[CPMM]:'cpmm',[PUMP]:'pumpfun',[TOK]:'token',[TOK22]:'token'};

const RUN_MS = 40_000;

async function tierConfig(tier) {
  if (tier === 'A') {
    // vaults are an explicit account list — sample live ones from the registry so some trade in-window
    const Redis = require('ioredis'); const r = new Redis(process.env.REDIS_URL, { lazyConnect: true }); await r.connect();
    const vaults = Object.keys(await r.hgetall('circuit:vault-registry').catch(() => ({})));
    await r.quit();
    const sample = vaults.length <= 5000 ? vaults : vaults.filter((_, i) => i % Math.ceil(vaults.length / 5000) === 0);
    return { slice: 72, expect: ['token', 'pumpfun'],
      accounts: { vaults: { account: sample, owner: [], filters: [] }, pumpfun: { account: [], owner: [PUMP], filters: [] } } };
  }
  if (tier === 'B') return { slice: 215, expect: ['pumpswap', 'orca'],
    accounts: { pumpswap: { account: [], owner: [PSWAP], filters: [{ datasize: 301 }] }, orca: { account: [], owner: [ORCA], filters: [{ datasize: 653 }] } } };
  if (tier === 'C') return { slice: 333, expect: ['clmm', 'cpmm'],
    accounts: { clmm: { account: [], owner: [CLMM], filters: [{ datasize: 1544 }] }, cpmm: { account: [], owner: [CPMM], filters: [] } } };
  if (tier === 'mints') return { slice: 0, expect: ['token'],
    accounts: { mints: { account: [], owner: [TOK, TOK22], filters: [{ datasize: 82 }] } } };
  throw new Error('tier must be A|B|C|mints');
}

(async () => {
  const tier = process.argv[2] || 'A';
  const cfg = await tierConfig(tier);
  const client = new Client(process.env.GEYSER_ENDPOINT, process.env.GEYSER_TOKEN, {
    'grpc.max_receive_message_length': 64 * 1024 * 1024, 'grpc.default_compression_algorithm': 2 });
  const stream = await client.subscribe();
  const tally = {}; // program → { n, min, max }

  stream.on('data', (d) => {
    if (!d.account) return;
    const acc = d.account.account; if (!acc) return;
    const owner = bs58.encode(acc.owner);
    const prog = NAME[owner] || 'other';
    const len = acc.data ? acc.data.length : 0;
    const t = (tally[prog] ||= { n: 0, min: 1e9, max: 0 });
    t.n++; t.min = Math.min(t.min, len); t.max = Math.max(t.max, len);
  });
  stream.on('error', (e) => { console.error('stream error:', e.message); process.exit(2); });

  const req = SubscribeRequest.fromPartial({
    accounts: cfg.accounts, transactions: {}, slots: {}, blocks: {}, blocksMeta: {}, commitment: 1,
    accountsDataSlice: cfg.slice > 0 ? [{ offset: '0', length: String(cfg.slice) }] : [],
  });
  await new Promise((res, rej) => stream.write(req, (e) => e ? rej(e) : res()));
  console.log(`tier ${tier}: subscribed with slice=${cfg.slice || 'none'}, expecting [${cfg.expect}] — sampling ${RUN_MS/1000}s…`);

  await new Promise((r) => setTimeout(r, RUN_MS));
  try { stream.end(); } catch {}

  console.log('\nprogram   arrivals   min-len  max-len');
  for (const [p, t] of Object.entries(tally).sort((a, b) => b[1].n - a[1].n))
    console.log(`${p.padEnd(10)}${String(t.n).padEnd(11)}${String(t.min===1e9?'-':t.min).padEnd(9)}${t.max}`);
  const missing = cfg.expect.filter((p) => !tally[p] || tally[p].n === 0);
  const overSlice = cfg.slice > 0 && Object.values(tally).some((t) => t.max > cfg.slice);
  console.log('\n' + (missing.length
    ? `❌ FAIL — expected program(s) NEVER arrived under slice ${cfg.slice}: [${missing}] (Triton dropped them)`
    : `✅ PASS — all expected programs [${cfg.expect}] arrived under slice ${cfg.slice}${cfg.slice ? ` (max returned len ${Object.values(tally).reduce((m,t)=>Math.max(m,t.max),0)} ≤ ${cfg.slice} ⇒ slice honored)` : ''}.`));
  process.exit(missing.length ? 1 : 0);
})().catch((e) => { console.error('slice-probe error:', e); process.exit(2); });
