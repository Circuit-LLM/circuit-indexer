// parsers/orca.js — Parse Orca Whirlpool CLMM pool state.
//
// Program: whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc
// Account: Whirlpool, 653 bytes
//
// Layout (Anchor discriminator + struct):
//   [0-7]   discriminator
//   [8-39]  whirlpools_config  (Pubkey)
//   [40]    whirlpool_bump     (u8)
//   [41-42] tick_spacing       (u16)
//   [43-44] tick_spacing_seed  (u16)
//   [45-46] fee_rate           (u16)
//   [47-48] protocol_fee_rate  (u16)
//   [49-64] liquidity          (u128)
//   [65-80] sqrt_price         (u128) ← Q64.64 price
//   [81-84] tick_current_index (i32)
//   [85-92] protocol_fee_owed_a (u64)
//   [93-100] protocol_fee_owed_b (u64)
//   [101-132] token_mint_a     (Pubkey)
//   [133-164] token_vault_a    (Pubkey)
//   [165-180] fee_growth_global_a (u128)
//   [181-212] token_mint_b     (Pubkey)
//   [213-244] token_vault_b    (Pubkey)
//   [245-260] fee_growth_global_b (u128)
//   [261-268] reward_last_updated_timestamp (u64)
//   [269-652] reward_infos     (3 × 128 bytes)
'use strict';

const bs58 = require('bs58').default ?? require('bs58');

const ORCA_WHIRLPOOL_PROGRAM = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc';
const WHIRLPOOL_SIZE         = 653;

const WP = {
  DISCRIMINATOR:      0,
  WHIRLPOOLS_CFG:     8,
  WHIRLPOOL_BUMP:     40,
  TICK_SPACING:       41,
  TICK_SPACING_SEED:  43,
  FEE_RATE:           45,
  PROTOCOL_FEE_RATE:  47,
  LIQUIDITY:          49,
  SQRT_PRICE:         65,
  TICK_CURRENT_INDEX: 81,
  TOKEN_MINT_A:       101, // Pubkey
  TOKEN_VAULT_A:      133, // Pubkey
  TOKEN_MINT_B:       181, // Pubkey
  TOKEN_VAULT_B:      213, // Pubkey
};

// SHA256("account:Whirlpool")[0..8]
const WHIRLPOOL_DISCRIMINATOR = Buffer.from([63, 149, 209, 12, 225, 128, 99, 9]);

/**
 * price = (sqrtPriceX64 / 2^64)^2 × 10^(decimalsB - decimalsA)
 */
function whirlpoolPrice(sqrtPriceX64, decimalsA, decimalsB) {
  if (sqrtPriceX64 === 0n) return 0;
  const Q64   = 2n ** 64n;
  const ratio = Number(sqrtPriceX64) / Number(Q64);
  // price = raw_B/raw_A adjusted to human: multiply by 10^(decA - decB)
  return ratio * ratio * Math.pow(10, decimalsA - decimalsB);
}

function parseWhirlpool(buf, decimalsA = null, decimalsB = null) {
  if (buf.length < WHIRLPOOL_SIZE) return null;
  if (!buf.slice(0, 8).equals(WHIRLPOOL_DISCRIMINATOR)) return null;

  try {
    const feeRate     = buf.readUInt16LE(WP.FEE_RATE);
    const tickSpacing = buf.readUInt16LE(WP.TICK_SPACING);
    const tickCurrent = buf.readInt32LE(WP.TICK_CURRENT_INDEX);

    const liq_lo    = buf.readBigUInt64LE(WP.LIQUIDITY);
    const liq_hi    = buf.readBigUInt64LE(WP.LIQUIDITY + 8);
    const liquidity = liq_lo | (liq_hi << 64n);

    const sqrt_lo      = buf.readBigUInt64LE(WP.SQRT_PRICE);
    const sqrt_hi      = buf.readBigUInt64LE(WP.SQRT_PRICE + 8);
    const sqrtPriceX64 = sqrt_lo | (sqrt_hi << 64n);

    const mintA = bs58.encode(buf.slice(WP.TOKEN_MINT_A, WP.TOKEN_MINT_A + 32));
    const mintB = bs58.encode(buf.slice(WP.TOKEN_MINT_B, WP.TOKEN_MINT_B + 32));

    const price = (decimalsA !== null && decimalsB !== null)
      ? whirlpoolPrice(sqrtPriceX64, decimalsA, decimalsB)
      : null;

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
      feeRate,
      feePct:       feeRate / 10000 / 100,
      price,
    };
  } catch { return null; }
}

function processAccountEvent(event) {
  if (event.type !== 'account') return null;
  if (event.owner !== ORCA_WHIRLPOOL_PROGRAM) return null;

  let buf;
  try {
    buf = Buffer.from(bs58.decode(event.data));
  } catch { return null; }

  const pool = parseWhirlpool(buf, null, null);
  if (!pool) return null;

  return { poolAccount: event.pubkey, slot: event.slot, ts: event.ts, ...pool };
}

module.exports = {
  processAccountEvent, parseWhirlpool, whirlpoolPrice,
  ORCA_WHIRLPOOL_PROGRAM, WHIRLPOOL_DISCRIMINATOR,
};
