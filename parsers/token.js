// parsers/token.js — Detect new token mints and decode token account state.
//
// A new token appears when we see a transaction from the Token or Token-2022
// program that includes a new account with zero previous lamports.
// We use base58-decoded data to detect the account type by its discriminator.
//
// Token Mint layout (82 bytes for spl-token):
//   [0..4]   mint_authority option (coption: 4 bytes)
//   [4..8]   mint_authority pubkey (if present)
//   [36..44] supply (u64, little-endian)
//   [44]     decimals (u8)
//   [45]     is_initialized (bool)
//   [46..50] freeze_authority option
//   [50..82] freeze_authority pubkey (if present)
'use strict';

const bs58 = require('bs58').default ?? require('bs58');

const TOKEN_PROGRAM      = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const TOKEN_2022_PROGRAM = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
const MINT_DATA_SIZE     = 82;

// CoOption (Solana serialization): first 4 bytes are 0 (None) or 1 (Some)
function readCoOption(buf, offset) {
  const tag = buf.readUInt32LE(offset);
  if (tag === 0) return { value: null, next: offset + 4 };
  return { value: buf.slice(offset + 4, offset + 36), next: offset + 36 };
}

/**
 * Parse a token mint account from raw base58 data.
 * Returns null if the data is not a recognizable mint.
 */
function parseMint(dataB58, owner) {
  if (owner !== TOKEN_PROGRAM && owner !== TOKEN_2022_PROGRAM) return null;

  let buf;
  try {
    buf = Buffer.from(bs58.decode(dataB58));
  } catch {
    return null;
  }

  // Token-2022 accounts have extensions after byte 82, so >= 82 is valid
  if (buf.length < MINT_DATA_SIZE) return null;

  try {
    const mintAuthOpt    = readCoOption(buf, 0);
    const supply         = buf.readBigUInt64LE(36);
    const decimals       = buf.readUInt8(44);
    const isInitialized  = buf.readUInt8(45) === 1;

    if (!isInitialized) return null;

    const freezeAuthOpt  = readCoOption(buf, 46);

    const mintAuthority   = mintAuthOpt.value
      ? bs58.encode(mintAuthOpt.value) : null;
    const freezeAuthority = freezeAuthOpt.value
      ? bs58.encode(freezeAuthOpt.value) : null;

    return {
      type:            'mint',
      tokenProgram:    owner === TOKEN_2022_PROGRAM ? 'token-2022' : 'spl-token',
      mintAuthority,
      freezeAuthority,
      supply:          supply.toString(),
      decimals,
      isInitialized,
    };
  } catch {
    return null;
  }
}

/**
 * Process a Geyser account event — if it's a new token mint, return mint info.
 * Returns null for non-mint accounts or accounts that aren't new.
 *
 * @param {object} event - Geyser account event
 * @returns {object|null} - { mint: pubkey, ...mintInfo } or null
 */
function processAccountEvent(event) {
  if (event.type !== 'account') return null;

  const mintInfo = parseMint(event.data, event.owner);
  if (!mintInfo) return null;

  return {
    mint:     event.pubkey,
    slot:     event.slot,
    ts:       event.ts,
    lamports: event.lamports,
    ...mintInfo,
  };
}

module.exports = { parseMint, processAccountEvent, TOKEN_PROGRAM, TOKEN_2022_PROGRAM };
