// parsers/orca.js — Parse Orca Whirlpool CLMM pool state.
//
// Orca Whirlpools use the same CLMM math as Raydium but different account layout.
// Price: sqrt_price is stored as a Q64.64 fixed-point number (sqrtPriceX64).
//
// Program: whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc
// Account type: Whirlpool (653 bytes)
//
// Layout reference:
// https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/state/whirlpool.rs
'use strict';

const bs58 = require('bs58').default ?? require('bs58');

const ORCA_WHIRLPOOL_PROGRAM = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc';
const WHIRLPOOL_SIZE         = 653;

// Byte offsets in Whirlpool account
const WP = {
  DISCRIMINATOR:    0,   // [u8; 8]  — Anchor discriminator
  WHIRLPOOLS_CFG:   8,   // Pubkey   — WhirlpoolsConfig
  WHIRLPOOL_BUMP:   40,  // u8
  TICK_SPACING:     41,  // u16
  TICK_SPACING_SEED: 43, // u16
  FEE_RATE:         45,  // u16      — fee in hundredths of a bip (e.g. 300 = 0.03%)
  PROTOCOL_FEE_RATE: 47, // u16
  LIQUIDITY:        49,  // u128
  SQRT_PRICE:       65,  // u128     — Q64.64
  TICK_CURRENT_INDEX: 81,// i32
  FEE_GROWTH_A:     85,  // u128
  FEE_GROWTH_B:     101, // u128
  REWARD_LAST_UPDATED: 117, // u64
  // reward_infos: 3 × 128 bytes each (skipping)
  TOKEN_MINT_A:     357, // Pubkey
  TOKEN_MINT_B:     389, // Pubkey
  TOKEN_VAULT_A:    421, // Pubkey
  TOKEN_VAULT_B:    453, // Pubkey
  DECIMALS_A:       485, // u8 (not in standard layout — inferred from pool)
};

// Discriminator for Whirlpool accounts (first 8 bytes of SHA256("account:Whirlpool"))
const WHIRLPOOL_DISCRIMINATOR = Buffer.from([63, 149, 209, 12, 225, 128, 99, 9]);

/**
 * Compute price from Orca's sqrtPriceX64 (Q64.64 fixed-point).
 * price = (sqrtPriceX64 / 2^64)^2 × 10^(decimalsB - decimalsA)
 */
function whirlpoolPrice(sqrtPriceX64, decimalsA, decimalsB) {
  const Q64 = 2n ** 64n;
  const ratio = Number(sqrtPriceX64) / Number(Q64);
  return ratio * ratio * Math.pow(10, decimalsB - decimalsA);
}

/**
 * Parse Orca Whirlpool account buffer.
 * Note: decimalsA and decimalsB are NOT stored in the Whirlpool account itself —
 * they come from the token mint accounts. Pass them in if known; otherwise null.
 */
function parseWhirlpool(buf, decimalsA = null, decimalsB = null) {
  if (buf.length < WHIRLPOOL_SIZE) return null;

  // Verify discriminator
  if (!buf.slice(0, 8).equals(WHIRLPOOL_DISCRIMINATOR)) return null;

  try {
    const feeRate       = buf.readUInt16LE(WP.FEE_RATE);
    const tickSpacing   = buf.readUInt16LE(WP.TICK_SPACING);
    const tickCurrent   = buf.readInt32LE(WP.TICK_CURRENT_INDEX);

    // u128 little-endian: split into lo + hi 64-bit halves
    const liq_lo        = buf.readBigUInt64LE(WP.LIQUIDITY);
    const liq_hi        = buf.readBigUInt64LE(WP.LIQUIDITY + 8);
    const liquidity     = liq_lo | (liq_hi << 64n);

    const sqrt_lo       = buf.readBigUInt64LE(WP.SQRT_PRICE);
    const sqrt_hi       = buf.readBigUInt64LE(WP.SQRT_PRICE + 8);
    const sqrtPriceX64  = sqrt_lo | (sqrt_hi << 64n);

    const mintA = bs58.encode(buf.slice(WP.TOKEN_MINT_A, WP.TOKEN_MINT_A + 32));
    const mintB = bs58.encode(buf.slice(WP.TOKEN_MINT_B, WP.TOKEN_MINT_B + 32));

    const price = (decimalsA !== null && decimalsB !== null)
      ? whirlpoolPrice(sqrtPriceX64, decimalsA, decimalsB)
      : null; // price unknown until we have decimals from mint accounts

    return {
      type:         'orca-whirlpool',
      mintA,
      mintB,
      decimalsA,
      decimalsB,
      sqrtPriceX64: sqrtPriceX64.toString(),
      liquidity:    liquidity.toString(),
      tickCurrent,
      tickSpacing,
      feeRate,      // fee in hundredths of a bip
      feePct:       feeRate / 10000 / 100, // e.g. 0.0003 = 0.03%
      price,        // null if decimals not available
    };
  } catch { return null; }
}

/**
 * Process a Geyser account event — if it's an Orca Whirlpool, decode pool state.
 */
function processAccountEvent(event, decimalsMap = null) {
  if (event.type !== 'account') return null;
  if (event.owner !== ORCA_WHIRLPOOL_PROGRAM) return null;

  let buf;
  try {
    buf = Buffer.from(bs58.decode(event.data));
  } catch { return null; }

  // Look up decimals from our in-memory decimals map if available
  let decA = null, decB = null;
  // We'll fill this in from the indexer once we've seen the mint accounts

  const pool = parseWhirlpool(buf, decA, decB);
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
  parseWhirlpool,
  whirlpoolPrice,
  ORCA_WHIRLPOOL_PROGRAM,
  WHIRLPOOL_DISCRIMINATOR,
};
