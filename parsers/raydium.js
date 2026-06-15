// parsers/raydium.js — Parse Raydium AMM v4 and CLMM pool account state.
//
// Program IDs:
//   AMM v4:  675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8
//   CLMM:    CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK
//
// AMM v4 (constant-product): price = pc_reserve / coin_reserve (decimal-adjusted)
//   Account: AmmInfo, 752 bytes
//
// CLMM (concentrated liquidity): price = (sqrtPriceX64 / 2^64)^2 × 10^(dec1-dec0)
//   Account: PoolState, 1544 bytes
//   Layout includes 1-byte `bump` field at offset 8 (after discriminator),
//   which shifts all subsequent fields by 1 vs a naive discriminator-only layout.
'use strict';

const bs58 = require('bs58').default ?? require('bs58');
const { toBuf } = require('../lib/databuf');

const RAYDIUM_AMM_V4 = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8';
const RAYDIUM_CLMM   = 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK';

const AMM_INFO_SIZE  = 752;
const CLMM_POOL_SIZE = 1544;

// ── AMM v4 — AmmInfo layout offsets ──────────────────────────────────────────
const AMM = {
  STATUS:        0,   // u64 — 6 = initialized pool
  COIN_DECIMALS: 32,  // u64
  PC_DECIMALS:   40,  // u64
  COIN_AMOUNT:   88,  // u64 — coin vault reserve
  PC_AMOUNT:     96,  // u64 — pc vault reserve
  COIN_MINT:     264, // Pubkey (32 bytes)
  PC_MINT:       296, // Pubkey (32 bytes)
};

// ── CLMM — PoolState layout offsets ──────────────────────────────────────────
// discriminator[8] + bump[1] + amm_config[32] + owner[32] = token_mint_0 at 73
const CLMM = {
  DISCRIMINATOR:   0,   // [u8; 8]
  BUMP:            8,   // u8 — 1-byte bump shifts every subsequent field by 1
  AMML_CONFIG:     9,   // Pubkey (32)
  POOL_CREATOR:    41,  // Pubkey (32)
  MINT_0:          73,  // Pubkey (32) — token_mint_0
  MINT_1:          105, // Pubkey (32) — token_mint_1
  TOKEN_VAULT_0:   137, // Pubkey (32)
  TOKEN_VAULT_1:   169, // Pubkey (32)
  OBSERVATION_KEY: 201, // Pubkey (32)
  MINT_DECIMALS_0: 233, // u8
  MINT_DECIMALS_1: 234, // u8
  TICK_SPACING:    235, // u16
  LIQUIDITY:       237, // u128 (16 bytes)
  SQRT_PRICE_X64:  253, // u128 (16 bytes)
  TICK_CURRENT:    269, // i32
};

// ── Price math ────────────────────────────────────────────────────────────────

function ammPrice(coinAmount, pcAmount, coinDecimals, pcDecimals) {
  if (coinAmount === 0n) return null;
  const priceRaw = Number(pcAmount) / Number(coinAmount);
  return priceRaw * Math.pow(10, Number(coinDecimals) - Number(pcDecimals));
}

function clmmPrice(sqrtPriceX64, decimals0, decimals1) {
  if (sqrtPriceX64 === 0n) return 0;
  const Q64   = 2n ** 64n;
  const ratio = Number(sqrtPriceX64) / Number(Q64);
  // price = raw_1/raw_0 adjusted to human: multiply by 10^(dec0 - dec1)
  return ratio * ratio * Math.pow(10, decimals0 - decimals1);
}

// ── Parsers ───────────────────────────────────────────────────────────────────

function parseAmmV4(buf) {
  if (buf.length < AMM_INFO_SIZE) return null;
  try {
    const status = buf.readBigUInt64LE(AMM.STATUS);
    // Only parse initialized pools (status == 6 or 7)
    if (status !== 6n && status !== 7n) return null;

    const coinDecimals = buf.readBigUInt64LE(AMM.COIN_DECIMALS);
    const pcDecimals   = buf.readBigUInt64LE(AMM.PC_DECIMALS);
    const coinAmount   = buf.readBigUInt64LE(AMM.COIN_AMOUNT);
    const pcAmount     = buf.readBigUInt64LE(AMM.PC_AMOUNT);
    const coinMint     = bs58.encode(buf.slice(AMM.COIN_MINT, AMM.COIN_MINT + 32));
    const pcMint       = bs58.encode(buf.slice(AMM.PC_MINT,   AMM.PC_MINT   + 32));

    const price = ammPrice(coinAmount, pcAmount, coinDecimals, pcDecimals);

    return {
      type:         'raydium-amm-v4',
      coinMint,
      pcMint,
      coinReserve:  coinAmount.toString(),
      pcReserve:    pcAmount.toString(),
      coinDecimals: Number(coinDecimals),
      pcDecimals:   Number(pcDecimals),
      price,
      status:       Number(status),
    };
  } catch { return null; }
}

function parseClmm(buf) {
  if (buf.length < CLMM_POOL_SIZE) return null;
  try {
    const decimals0 = buf.readUInt8(CLMM.MINT_DECIMALS_0);
    const decimals1 = buf.readUInt8(CLMM.MINT_DECIMALS_1);

    const sqrt_lo      = buf.readBigUInt64LE(CLMM.SQRT_PRICE_X64);
    const sqrt_hi      = buf.readBigUInt64LE(CLMM.SQRT_PRICE_X64 + 8);
    const sqrtPriceX64 = sqrt_lo | (sqrt_hi << 64n);

    const liq_lo    = buf.readBigUInt64LE(CLMM.LIQUIDITY);
    const liq_hi    = buf.readBigUInt64LE(CLMM.LIQUIDITY + 8);
    const liquidity = liq_lo | (liq_hi << 64n);

    const tickCurrent = buf.readInt32LE(CLMM.TICK_CURRENT);
    const tickSpacing = buf.readUInt16LE(CLMM.TICK_SPACING);

    const mint0 = bs58.encode(buf.slice(CLMM.MINT_0, CLMM.MINT_0 + 32));
    const mint1 = bs58.encode(buf.slice(CLMM.MINT_1, CLMM.MINT_1 + 32));

    // Reject tick arrays and other non-pool CLMM accounts (all-zero mints, zero sqrtPrice)
    const SYSTEM_PROGRAM = '11111111111111111111111111111111';
    if (mint0 === SYSTEM_PROGRAM || mint1 === SYSTEM_PROGRAM) return null;
    if (sqrtPriceX64 === 0n) return null;
    if (tickSpacing === 0) return null;
    if (decimals0 > 18 || decimals1 > 18) return null;

    const price = clmmPrice(sqrtPriceX64, decimals0, decimals1);

    return {
      type:         'raydium-clmm',
      mint0,
      mint1,
      decimals0,
      decimals1,
      sqrtPriceX64: sqrtPriceX64.toString(),
      liquidity:    liquidity.toString(),
      tickCurrent,
      tickSpacing,
      price,
    };
  } catch { return null; }
}

function processAccountEvent(event) {
  if (event.type !== 'account') return null;
  // Owner guard: skip the costly parse unless this account belongs to a Raydium program.
  // (Other parsers already guard on owner; raydium did not, so it processed every event.)
  if (event.owner !== RAYDIUM_AMM_V4 && event.owner !== RAYDIUM_CLMM) return null;

  let buf;
  try {
    buf = toBuf(event.data);
  } catch { return null; }

  let pool = null;
  if (event.owner === RAYDIUM_AMM_V4) {
    pool = parseAmmV4(buf);
  } else if (event.owner === RAYDIUM_CLMM) {
    pool = parseClmm(buf);
  }
  if (!pool) return null;

  return { poolAccount: event.pubkey, slot: event.slot, ts: event.ts, ...pool };
}

module.exports = { processAccountEvent, parseAmmV4, parseClmm, RAYDIUM_AMM_V4, RAYDIUM_CLMM };
