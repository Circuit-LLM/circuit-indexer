#!/usr/bin/env node
// shadow-run.js — validate the multi-stream refactor end-to-end WITHOUT touching production.
// Runs a second indexer in CIRCUIT_STREAM_SLICING=1 mode with:
//   • REDIS_URL   → scratch DB /1 (seeded with the live vault-registry so CPMM/PumpSwap price fast)
//   • DATABASE_URL→ a nonexistent DB → Postgres writer falls to no-op (never writes prod PG)
// After it warms up, compare DB1 vs live DB0 with: node tools/parity-snapshot.js <label> (see SHADOW_REDIS_URL).
// Usage: node tools/shadow-run.js [seconds=300]
'use strict';
const path = require('path'), fs = require('fs'), { spawn } = require('child_process');
for (const l of fs.readFileSync(path.join(__dirname, '..', '.env'), 'utf8').split('\n')) {
  const s = l.trim(); if (!s || s[0] === '#') continue; const i = s.indexOf('='); if (i < 0) continue;
  const k = s.slice(0, i).trim(); let v = s.slice(i + 1).trim();
  if ((v[0] === '"' && v.endsWith('"')) || (v[0] === "'" && v.endsWith("'"))) v = v.slice(1, -1);
  if (!(k in process.env)) process.env[k] = v;
}
const SCRATCH = '/tmp/claude-1001/-home-watchtower/66e9804c-32c2-46e4-9834-79ac7da43a28/scratchpad';
const DUR = (Number(process.argv[2]) || 300) * 1000;
const db0 = process.env.REDIS_URL;
const db1 = db0.replace(/\/\d+$/, '') + '/1';

(async () => {
  const Redis = require('ioredis');
  const r0 = new Redis(db0, { lazyConnect: true }); await r0.connect();
  const r1 = new Redis(db1, { lazyConnect: true }); await r1.connect();
  await r1.flushdb();
  const reg = await r0.hgetall('circuit:vault-registry').catch(() => ({}));
  const entries = Object.entries(reg);
  for (let i = 0; i < entries.length; i += 1000) await r1.hset('circuit:vault-registry', ...entries.slice(i, i + 1000).flat());
  console.log(`[shadow] seeded scratch DB1 with ${entries.length} vaults`);
  await r0.quit(); await r1.quit();

  const mode = process.argv[3] || 'sliced';   // 'sliced' (multi-stream) | 'unsliced' (current default)
  const env = { ...process.env,
    REDIS_URL: db1,
    DATABASE_URL: 'postgresql://localhost/circuit_index_SHADOW_NONEXISTENT',  // → PG no-op, no prod writes
    CIRCUIT_NARROW: '1', CIRCUIT_COST_PROBE: '1',
  };
  if (mode === 'sliced') env.CIRCUIT_STREAM_SLICING = '1'; else delete env.CIRCUIT_STREAM_SLICING;
  console.log(`[shadow] mode=${mode}`);
  const log = fs.openSync(path.join(SCRATCH, 'shadow-indexer.log'), 'w');
  const child = spawn('node', ['indexer.js', '--consumer=grpc'], { cwd: path.join(__dirname, '..'), env, stdio: ['ignore', log, log] });
  try { fs.writeFileSync(path.join(SCRATCH, 'shadow.pid'), String(child.pid)); } catch {}
  console.log(`[shadow] indexer PID ${child.pid}, mode=${mode} → DB1, ${DUR/1000}s (log: ${SCRATCH}/shadow-indexer.log)`);
  console.log(`[shadow] compare with:  SHADOW=1 node tools/parity-snapshot.js shadow   (point at DB1)`);
  const stop = () => { try { child.kill('SIGTERM'); } catch {} setTimeout(() => { try { child.kill('SIGKILL'); } catch {} process.exit(0); }, 4000); };
  setTimeout(() => { console.log('[shadow] stopping indexer'); stop(); }, DUR);
  child.on('exit', (c) => console.log(`[shadow] indexer exited (${c})`));
})().catch((e) => { console.error('shadow-run error:', e); process.exit(1); });
