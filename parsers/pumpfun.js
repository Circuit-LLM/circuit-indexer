// parsers/pumpfun.js — Parse Pump.fun bonding curve account state.
//
// Program: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
//
// BondingCurve account layout (Anchor discriminator + struct):
//   [0-7]   discriminator  (8 bytes)
//   [8-15]  virtualTokenReserves  u64
//   [16-23] virtualSolReserves    u64
//   [24-31] realTokenReserves     u64
//   [32-39] realSolReserves       u64
//   [40-47] tokenTotalSupply      u64
//   [48]    complete              bool
//
// The mint address is NOT stored in the bonding curve account — it is the PDA seed.
// The pool key in Redis is circuit:pool:{bcAddress}. The reverse index
// circuit:pool-by-mint:{mint} is written by circuit-price-feed's /register endpoint
// when a circuit-agent registers a newly scanned Pump.fun token.
//
// Price = virtualSolReserves / virtualTokenReserves (raw lamports per raw token).
// Human price = (vSol / 1e9) / (vToken / 10^decimals).
// Decimals are NOT stored here — use circuit:mint:{mint}.decimals or default 6.
'use strict';
const { toBuf } = require('../lib/databuf');

const PUMP_PROGRAM  = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const BC_DATA_SIZE  = 49; // 8 discriminator + 5×u64 + 1 bool

const BC = {
  VIRTUAL_TOKEN: 8,
  VIRTUAL_SOL:   16,
  REAL_TOKEN:    24,
  REAL_SOL:      32,
  TOTAL_SUPPLY:  40,
  COMPLETE:      48,
};

function parseBondingCurve(buf) {
  if (buf.length < BC_DATA_SIZE) return null;
  try {
    const virtualTokenReserves = buf.readBigUInt64LE(BC.VIRTUAL_TOKEN);
    const virtualSolReserves   = buf.readBigUInt64LE(BC.VIRTUAL_SOL);
    const realTokenReserves    = buf.readBigUInt64LE(BC.REAL_TOKEN);
    const realSolReserves      = buf.readBigUInt64LE(BC.REAL_SOL);
    const tokenTotalSupply     = buf.readBigUInt64LE(BC.TOTAL_SUPPLY);
    const complete             = buf.readUInt8(BC.COMPLETE) !== 0;

    if (virtualTokenReserves === 0n) return null;

    // Raw SOL-per-raw-token price (no decimal adjustment — price-feed applies decimals).
    // priceRaw = lamports per raw token unit.
    const priceRaw = Number(virtualSolReserves) / Number(virtualTokenReserves);

    return {
      type:                  'pump-bonding-curve',
      virtualTokenReserves:  virtualTokenReserves.toString(),
      virtualSolReserves:    virtualSolReserves.toString(),
      realTokenReserves:     realTokenReserves.toString(),
      realSolReserves:       realSolReserves.toString(),
      tokenTotalSupply:      tokenTotalSupply.toString(),
      complete,
      priceRaw, // lamports per raw token — multiply by 10^decimals / 1e9 to get SOL per UI token
    };
  } catch { return null; }
}

function processAccountEvent(event) {
  if (event.type !== 'account') return null;
  if (event.owner !== PUMP_PROGRAM) return null;

  let buf;
  try {
    buf = toBuf(event.data);
  } catch { return null; }

  const pool = parseBondingCurve(buf);
  if (!pool) return null;

  return { poolAccount: event.pubkey, slot: event.slot, ts: event.ts, ...pool };
}

module.exports = { processAccountEvent, parseBondingCurve, PUMP_PROGRAM, BC_DATA_SIZE };
