// parsers/cpmm.js — Parse Raydium CPMM (constant-product) pool state.
//
// Program: CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C
//
// PoolState layout (Anchor discriminator, NO extra bump prefix — unlike CLMM):
//   [0-7]    discriminator
//   [8-39]   amm_config     (Pubkey)
//   [40-71]  pool_creator   (Pubkey)
//   [72-103] token0_vault   (Pubkey)
//   [104-135] token1_vault  (Pubkey)
//   [136-167] lp_mint       (Pubkey)
//   [168-199] token0_mint   (Pubkey)
//   [200-231] token1_mint   (Pubkey)
//   [232-263] token0_program (Pubkey)
//   [264-295] token1_program (Pubkey)
//   [296-327] observation_key (Pubkey)
//   [328]    auth_bump      (u8)
//   [329]    status         (u8) — 0=disabled, 1=enabled
//   [330]    lp_mint_decimals (u8)
//   [331]    mint0_decimals (u8)
//   [332]    mint1_decimals (u8)
//
// Price comes from vault token balances, not pool state.
// The indexer tracks vault→pool mapping and recomputes price on vault updates.
'use strict';

const bs58 = require('bs58').default ?? require('bs58');
const { toBuf } = require('../lib/databuf');

const RAYDIUM_CPMM = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C';

const CPMM = {
  AMM_CONFIG:   8,   // Pubkey (32)
  POOL_CREATOR: 40,  // Pubkey (32)
  VAULT_0:      72,  // Pubkey (32) — token0_vault
  VAULT_1:      104, // Pubkey (32) — token1_vault
  LP_MINT:      136, // Pubkey (32)
  MINT_0:       168, // Pubkey (32) — token0_mint
  MINT_1:       200, // Pubkey (32) — token1_mint
  AUTH_BUMP:    328, // u8
  STATUS:       329, // u8 — 1 = enabled
  DEC_LP:       330, // u8
  DEC_0:        331, // u8 — mint0 decimals
  DEC_1:        332, // u8 — mint1 decimals
};

// SPL token account: u64 balance at offset 64
const TOKEN_BALANCE_OFFSET = 64;

function parseCpmm(buf) {
  if (buf.length < 333) return null;
  try {
    const status = buf.readUInt8(CPMM.STATUS);
    if (status !== 1) return null; // only enabled pools

    const mint0  = bs58.encode(buf.slice(CPMM.MINT_0,  CPMM.MINT_0  + 32));
    const mint1  = bs58.encode(buf.slice(CPMM.MINT_1,  CPMM.MINT_1  + 32));
    const vault0 = bs58.encode(buf.slice(CPMM.VAULT_0, CPMM.VAULT_0 + 32));
    const vault1 = bs58.encode(buf.slice(CPMM.VAULT_1, CPMM.VAULT_1 + 32));
    const dec0   = buf.readUInt8(CPMM.DEC_0);
    const dec1   = buf.readUInt8(CPMM.DEC_1);

    // Sanity: decimals should be reasonable, mints not system program
    if (dec0 > 18 || dec1 > 18) return null;
    if (mint0 === '11111111111111111111111111111111') return null;
    if (mint1 === '11111111111111111111111111111111') return null;

    return {
      type:  'raydium-cpmm',
      mint0,
      mint1,
      vault0,
      vault1,
      dec0,
      dec1,
      price: null, // computed when vault balances arrive
    };
  } catch { return null; }
}

function processAccountEvent(event) {
  if (event.type !== 'account') return null;
  if (event.owner !== RAYDIUM_CPMM) return null;

  let buf;
  try {
    buf = toBuf(event.data);
  } catch { return null; }

  const pool = parseCpmm(buf);
  if (!pool) return null;

  return { poolAccount: event.pubkey, slot: event.slot, ts: event.ts, ...pool };
}

module.exports = {
  processAccountEvent, parseCpmm,
  RAYDIUM_CPMM, CPMM, TOKEN_BALANCE_OFFSET,
};
