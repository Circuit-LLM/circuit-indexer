// parsers/pumpswap.js — Parse PumpSwap (pump.fun AMM) pool state.
//
// Program: pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA
//
// Pool account layout (301 bytes, verified on-chain):
//   [0-7]    discriminator = f19a6d0411b16dbc (8 bytes)
//   [8-39]   unknown pubkey                    (32 bytes)
//   [40-42]  unknown                           (3 bytes)
//   [43-74]  base_mint  (non-SOL token)        (32 bytes)
//   [75-106] quote_mint (WSOL)                 (32 bytes)
//   [107-138] unknown pubkey (LP mint?)        (32 bytes)
//   [139-170] base_vault  (token SPL account, spl_owner = pool) (32 bytes)
//   [171-202] quote_vault (WSOL SPL account,  spl_owner = pool) (32 bytes)
//   [203-300] additional fields (fees, config, etc.)
//
// Decimals for base_mint are NOT stored in the pool account — fetched via RPC.
// Price is computed from vault token balances, same as Raydium CPMM.
'use strict';

const bs58 = require('bs58').default ?? require('bs58');

const PUMPSWAP_PROGRAM = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
const DISCRIMINATOR    = Buffer.from('f19a6d0411b16dbc', 'hex');
const POOL_SIZE        = 301;

const OFFSETS = {
  BASE_MINT:   43,   // 32 bytes — non-SOL token mint
  QUOTE_MINT:  75,   // 32 bytes — WSOL
  BASE_VAULT:  139,  // 32 bytes — base token vault (spl_owner = pool address)
  QUOTE_VAULT: 171,  // 32 bytes — SOL vault       (spl_owner = pool address)
};

function parsePumpswap(buf) {
  if (buf.length < POOL_SIZE) return null;
  try {
    if (!buf.slice(0, 8).equals(DISCRIMINATOR)) return null;

    const baseMint   = bs58.encode(buf.slice(OFFSETS.BASE_MINT,   OFFSETS.BASE_MINT   + 32));
    const quoteMint  = bs58.encode(buf.slice(OFFSETS.QUOTE_MINT,  OFFSETS.QUOTE_MINT  + 32));
    const baseVault  = bs58.encode(buf.slice(OFFSETS.BASE_VAULT,  OFFSETS.BASE_VAULT  + 32));
    const quoteVault = bs58.encode(buf.slice(OFFSETS.QUOTE_VAULT, OFFSETS.QUOTE_VAULT + 32));

    if (baseMint  === '11111111111111111111111111111111') return null;
    if (quoteMint === '11111111111111111111111111111111') return null;

    return {
      type:       'pumpswap',
      baseMint,
      quoteMint,
      baseVault,
      quoteVault,
      price: null,
    };
  } catch { return null; }
}

function processAccountEvent(event) {
  if (event.type !== 'account') return null;
  if (event.owner !== PUMPSWAP_PROGRAM) return null;

  let buf;
  try {
    buf = Buffer.from(bs58.decode(event.data));
  } catch { return null; }

  const pool = parsePumpswap(buf);
  if (!pool) return null;

  return { poolAccount: event.pubkey, slot: event.slot, ts: event.ts, ...pool };
}

module.exports = {
  processAccountEvent, parsePumpswap,
  PUMPSWAP_PROGRAM, OFFSETS, DISCRIMINATOR, POOL_SIZE,
};
