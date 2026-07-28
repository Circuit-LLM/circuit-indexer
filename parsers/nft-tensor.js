// parsers/nft-tensor.js — Parse Tensor marketplace NFT listings (ListState).
//
// Program: TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp  (Tensor unified marketplace / "tcomp")
//
// ListState account layout (317 bytes, VERIFIED against a live account 2026-07-28):
//   [0-7]     anchor discriminator = 4ef2598aa1ddb04b  (sha256("account:ListState")[:8])
//   [8]       version   u8
//   [9]       bump      [u8;1]
//   [10-41]   owner     pubkey (32)   — the seller
//   [42-73]   assetId   pubkey (32)   — the listed NFT mint
//   [74-81]   amount    u64 LE (8)    — list price (native lamports when currency=None)
//   [82]      currency  Option tag: 0 = native SOL, 1 = SPL token follows (32B @83)
//   [83+]     currency pubkey / expiry / private-taker / broker / rentPayer / cosigner / reserved
//
// We read ONLY the fields at fixed offsets 10/42/74/82 — all of which sit BEFORE the first
// borsh Option (currency @82), so their positions are stable regardless of which Options are set.
// The collection is NOT stored on ListState (only assetId is); collection bucketing is resolved
// downstream (indexer.js) via a cache-forever, off-Helius mint→collection map — never in this parser.
//
// Pure decode, no RPC, no Redis. Mirrors parsers/pumpswap.js.
'use strict';

const bs58 = require('bs58').default ?? require('bs58');
const { toBuf } = require('../lib/databuf');

const TENSOR_PROGRAM = 'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp';

// Anchor account discriminators (sha256("account:<Name>")[:8]).
const LIST_DISC = Buffer.from('4ef2598aa1ddb04b', 'hex');

const LIST_SIZE = 317;   // full on-chain size — the gRPC dataSize filter uses this
const LIST_MIN_READ = 83; // max byte we read (currency tag @82) — relaxed so a sliced prefix still parses

const SYSTEM_ADDR = '11111111111111111111111111111111';

const OFFSETS = {
  OWNER:    10,   // 32 — seller
  ASSET_ID: 42,   // 32 — listed NFT mint
  AMOUNT:   74,   // u64 LE — price in native lamports (when currency=None)
  CURRENCY: 82,   // Option tag: 0 native SOL, 1 SPL token @83
};

function parseListState(buf) {
  if (buf.length < LIST_MIN_READ) return null;
  try {
    if (!buf.slice(0, 8).equals(LIST_DISC)) return null;

    const seller  = bs58.encode(buf.slice(OFFSETS.OWNER,    OFFSETS.OWNER    + 32));
    const assetId = bs58.encode(buf.slice(OFFSETS.ASSET_ID, OFFSETS.ASSET_ID + 32));
    if (assetId === SYSTEM_ADDR) return null;

    const priceLamports = buf.readBigUInt64LE(OFFSETS.AMOUNT);
    if (priceLamports <= 0n) return null;

    const currencyTag = buf[OFFSETS.CURRENCY];
    // Only native-SOL listings have a directly-comparable lamports price. SPL-currency
    // listings (currencyTag === 1) are flagged non-native so downstream floor logic can skip
    // them rather than mix denominations (their `amount` is in the SPL token's base units).
    const native = currencyTag === 0;

    return {
      type:  'nft-listing',
      assetId,
      seller,
      priceLamports: priceLamports.toString(),
      priceSol: native ? Number(priceLamports) / 1e9 : null,
      native,
    };
  } catch { return null; }
}

function processAccountEvent(event) {
  if (event.type !== 'account') return null;
  if (event.owner !== TENSOR_PROGRAM) return null;

  let buf;
  try {
    buf = toBuf(event.data);
  } catch { return null; }

  const listing = parseListState(buf);
  if (!listing) return null;

  return { listState: event.pubkey, slot: event.slot, ts: event.ts, ...listing };
}

module.exports = {
  processAccountEvent, parseListState,
  TENSOR_PROGRAM, LIST_DISC, LIST_SIZE, OFFSETS,
};
