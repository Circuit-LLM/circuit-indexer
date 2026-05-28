// parsers/raydium.js — Parse Raydium AMM and CLMM pool account state.
//
// Two Raydium pool types:
//
// 1. AMM v4 (constant-product): price = coin_reserve / pc_reserve
//    Program: 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8
//    Account layout: AmmInfo (752 bytes)
//
// 2. CLMM (concentrated liquidity): price = sqrtPriceX64^2 / 2^128
//    Program: CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK
//    Account layout: PoolState (1544 bytes)
//
// We decode pool state from the raw account data delivered by the Geyser plugin.
// The layouts are fixed — no RPC calls needed for price calculation.
'use strict';

const bs58 = require('bs58').default ?? require('bs58');

const RAYDIUM_AMM_V4   = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8';
const RAYDIUM_CLMM     = 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK';
const AMM_INFO_SIZE    = 752;
const CLMM_POOL_SIZE   = 1544;

// ── AMM v4 — AmmInfo layout offsets ──────────────────────────────────────────
// Reference: https://github.com/raydium-io/raydium-amm/blob/master/program/src/state.rs
// Key fields (byte offsets):
const AMM = {
  STATUS:           0,   // u64
  NONCE:            8,   // u64
  ORDER_NUM:        16,  // u64
  DEPTH:            24,  // u64
  COIN_DECIMALS:    32,  // u64
  PC_DECIMALS:      40,  // u64
  STATE:            48,  // u64
  RESET_FLAG:       56,  // u64
  MIN_SIZE:         64,  // u64
  VOL_MAX_CUT_RATIO: 72, // u64
  AMOUNT_WAVE:      80,  // u64
  COIN_AMOUNT:      88,  // u64 — coin reserve (token A)
  PC_AMOUNT:        96,  // u64 — pc reserve (token B = USDC/SOL)
  COIN_MINT:        264, // Pubkey (32 bytes)
  PC_MINT:          296, // Pubkey (32 bytes)
};

// ── CLMM — PoolState layout offsets ──────────────────────────────────────────
// Reference: https://github.com/raydium-io/raydium-clmm/blob/master/programs/amm/src/states/pool.rs
const CLMM = {
  DISCRIMINATOR:    0,   // [u8; 8]
  AMML_CONFIG:      8,   // Pubkey
  POOL_CREATOR:     40,  // Pubkey
  TOKEN_VAULT_0:    72,  // Pubkey
  TOKEN_VAULT_1:    104, // Pubkey
  OBSERVATION_KEY:  136, // Pubkey
  MINT_DECIMALS_0:  168, // u8
  MINT_DECIMALS_1:  169, // u8
  TICK_SPACING:     170, // u16
  LIQUIDITY:        172, // u128
  SQRT_PRICE_X64:   188, // u128 — encoded price
  TICK_CURRENT:     204, // i32
  MINT_0:           289, // Pubkey (32 bytes)
  MINT_1:           321, // Pubkey (32 bytes)
};

// ── Price math ────────────────────────────────────────────────────────────────

/**
 * AMM v4: price = pcAmount / coinAmount, adjusted for decimals.
 * This gives price of coin (token A) in terms of pc (token B, usually USDC or SOL).
 */
function ammPrice(coinAmount, pcAmount, coinDecimals, pcDecimals) {
  if (coinAmount === 0n) return null;
  const priceRaw = Number(pcAmount) / Number(coinAmount);
  return priceRaw * Math.pow(10, Number(coinDecimals) - Number(pcDecimals));
}

/**
 * CLMM: price = (sqrtPriceX64 / 2^64)^2
 * Gives price of token0 in terms of token1.
 */
function clmmPrice(sqrtPriceX64, decimals0, decimals1) {
  // sqrtPriceX64 is a u128 stored as two u64s (little-endian)
  // price = (sqrtPriceX64 / 2^64)^2 * 10^(decimals1 - decimals0)
  const Q64 = 2n ** 64n;
  const sqrt = sqrtPriceX64; // BigInt
  const ratio = Number(sqrt) / Number(Q64);
  const price = ratio * ratio * Math.pow(10, decimals1 - decimals0);
  return price;
}

// ── Parsers ───────────────────────────────────────────────────────────────────

function parseAmmV4(buf) {
  if (buf.length < AMM_INFO_SIZE) return null;
  try {
    const coinAmount   = buf.readBigUInt64LE(AMM.COIN_AMOUNT);
    const pcAmount     = buf.readBigUInt64LE(AMM.PC_AMOUNT);
    const coinDecimals = buf.readBigUInt64LE(AMM.COIN_DECIMALS);
    const pcDecimals   = buf.readBigUInt64LE(AMM.PC_DECIMALS);
    const status       = buf.readBigUInt64LE(AMM.STATUS);

    const coinMint = bs58.encode(buf.slice(AMM.COIN_MINT, AMM.COIN_MINT + 32));
    const pcMint   = bs58.encode(buf.slice(AMM.PC_MINT,   AMM.PC_MINT   + 32));

    const price = ammPrice(coinAmount, pcAmount, coinDecimals, pcDecimals);

    return {
      type:          'raydium-amm-v4',
      coinMint,
      pcMint,
      coinReserve:   coinAmount.toString(),
      pcReserve:     pcAmount.toString(),
      coinDecimals:  Number(coinDecimals),
      pcDecimals:    Number(pcDecimals),
      price,         // price of coinMint in pcMint units
      status:        Number(status),
    };
  } catch { return null; }
}

function parseClmm(buf) {
  if (buf.length < CLMM_POOL_SIZE) return null;
  try {
    const decimals0 = buf.readUInt8(CLMM.MINT_DECIMALS_0);
    const decimals1 = buf.readUInt8(CLMM.MINT_DECIMALS_1);

    // Read u128 as two u64s (little-endian)
    const lo = buf.readBigUInt64LE(CLMM.SQRT_PRICE_X64);
    const hi = buf.readBigUInt64LE(CLMM.SQRT_PRICE_X64 + 8);
    const sqrtPriceX64 = lo | (hi << 64n);

    const liquidity_lo = buf.readBigUInt64LE(CLMM.LIQUIDITY);
    const liquidity_hi = buf.readBigUInt64LE(CLMM.LIQUIDITY + 8);
    const liquidity    = liquidity_lo | (liquidity_hi << 64n);

    const tickCurrent = buf.readInt32LE(CLMM.TICK_CURRENT);
    const tickSpacing = buf.readUInt16LE(CLMM.TICK_SPACING);

    const mint0 = bs58.encode(buf.slice(CLMM.MINT_0, CLMM.MINT_0 + 32));
    const mint1 = bs58.encode(buf.slice(CLMM.MINT_1, CLMM.MINT_1 + 32));

    const price = clmmPrice(sqrtPriceX64, decimals0, decimals1);

    return {
      type:        'raydium-clmm',
      mint0,
      mint1,
      decimals0,
      decimals1,
      sqrtPriceX64: sqrtPriceX64.toString(),
      liquidity:   liquidity.toString(),
      tickCurrent,
      tickSpacing,
      price,       // price of mint0 in mint1 units
    };
  } catch { return null; }
}

/**
 * Process a Geyser account event — if it's a Raydium pool, decode and return pool state.
 */
function processAccountEvent(event) {
  if (event.type !== 'account') return null;

  let buf;
  try {
    buf = Buffer.from(bs58.decode(event.data));
  } catch { return null; }

  let pool = null;

  if (event.owner === RAYDIUM_AMM_V4) {
    pool = parseAmmV4(buf);
  } else if (event.owner === RAYDIUM_CLMM) {
    pool = parseClmm(buf);
  }

  if (!pool) return null;

  return {
    poolAccount: event.pubkey,
    slot:        event.slot,
    ts:          event.ts,
    ...pool,
  };
}

module.exports = {
  processAccountEvent,
  parseAmmV4,
  parseClmm,
  RAYDIUM_AMM_V4,
  RAYDIUM_CLMM,
};
