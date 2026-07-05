#!/usr/bin/env node
// resub-probe.js — READ-ONLY: does re-sending a SubscribeRequest make Triton re-SNAPSHOT the whole
// account set? The indexer re-subscribes every 8s as vaults register; if each re-subscribe re-snapshots
// all ~31k vaults, that repeated snapshot could dwarf the per-account slicing win.
// Method: subscribe to a fixed vault set, let the initial snapshot settle, measure this process's wire
// rate (/proc/self/io rchar) for a STATIC phase (no re-subscribe), then for a RE-SUBSCRIBE phase
// (re-send the same request every 8s). If phase-2 rate >> phase-1 rate → re-subscribe re-snapshots.
'use strict';
(function loadEnv(){const fs=require('fs'),path=require('path');try{for(const l of fs.readFileSync(path.join(__dirname,'..','.env'),'utf8').split('\n')){const s=l.trim();if(!s||s[0]==='#')continue;const i=s.indexOf('=');if(i<0)continue;const k=s.slice(0,i).trim();let v=s.slice(i+1).trim();if(!(k in process.env))process.env[k]=v;}}catch{}})();

const fs = require('fs');
const yg = require('@triton-one/yellowstone-grpc'); const Client = yg.default ?? yg;
const { SubscribeRequest } = require('@triton-one/yellowstone-grpc/dist/grpc/geyser');
const rchar = () => Number((fs.readFileSync('/proc/self/io', 'utf8').match(/rchar:\s*(\d+)/) || [])[1] || 0);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const gbday = (bytes, secs) => (bytes / secs * 86400 / 1073741824).toFixed(2);

(async () => {
  const N = Number(process.argv[2]) || 8000;
  const Redis = require('ioredis'); const r = new Redis(process.env.REDIS_URL, { lazyConnect: true }); await r.connect();
  const all = Object.keys(await r.hgetall('circuit:vault-registry').catch(() => ({})));
  await r.quit();
  const vaults = all.length <= N ? all : all.filter((_, i) => i % Math.ceil(all.length / N) === 0).slice(0, N);
  console.log(`resub-probe: ${vaults.length} vaults, UNSLICED. Measuring wire rate static vs 8s-re-subscribe.`);

  const client = new Client(process.env.GEYSER_ENDPOINT, process.env.GEYSER_TOKEN, {
    'grpc.max_receive_message_length': 64 * 1024 * 1024, 'grpc.default_compression_algorithm': 2 });
  const stream = await client.subscribe();
  let events = 0;
  stream.on('data', (d) => { if (d.account) events++; });
  stream.on('error', (e) => { console.error('stream error:', e.message); process.exit(2); });
  const req = () => SubscribeRequest.fromPartial({
    accounts: { vaults: { account: vaults, owner: [], filters: [] } },
    transactions: {}, slots: {}, blocks: {}, blocksMeta: {}, commitment: 1, accountsDataSlice: [] });
  await new Promise((res, rej) => stream.write(req(), (e) => e ? rej(e) : res()));

  await sleep(15000);   // let the INITIAL snapshot settle
  // Phase 1 — STATIC (no re-subscribe)
  let e0 = events, r0 = rchar(); await sleep(40000); let e1 = events, r1 = rchar();
  const p1bytes = r1 - r0, p1ev = e1 - e0;
  // Phase 2 — RE-SUBSCRIBE the same set every 8s
  const iv = setInterval(() => stream.write(req(), () => {}), 8000);
  let e2 = events, r2 = rchar(); await sleep(40000); let e3 = events, r3 = rchar();
  clearInterval(iv);
  const p2bytes = r3 - r2, p2ev = e3 - e2;
  try { stream.end(); } catch {}

  console.log(`\nPhase 1 STATIC        : ${gbday(p1bytes,40)} GB/day  (${p1ev} events, ${(p1ev/40).toFixed(0)} eps)`);
  console.log(`Phase 2 RE-SUBSCRIBE  : ${gbday(p2bytes,40)} GB/day  (${p2ev} events, ${(p2ev/40).toFixed(0)} eps)`);
  const ratio = p1bytes > 0 ? (p2bytes / p1bytes) : 0;
  console.log(`\nre-subscribe wire multiplier: ${ratio.toFixed(2)}x`);
  console.log(ratio > 1.5
    ? `❌ RE-SUBSCRIBE RE-SNAPSHOTS — every 8s re-subscribe re-sends the account set. This is a big egress lever: slow/batch re-subscribes.`
    : `✅ re-subscribe does NOT re-snapshot (Δ only) — the 8s re-subscribe is cheap; not the lever.`);
  process.exit(0);
})().catch((e) => { console.error('resub-probe error:', e); process.exit(2); });
