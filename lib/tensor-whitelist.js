// lib/tensor-whitelist.js — Resolve a Tensor WhitelistV2 account → its `voc` (verified on-chain
// collection = the Metaplex collection mint), so collection BIDS (keyed on-chain by the whitelist)
// can be joined to LISTINGS (keyed by the Metaplex collection mint).
//
// Tensor collection bids store `targetId` = a WhitelistV2 pubkey, NOT the collection mint. Only
// voc-type whitelists map cleanly to a Metaplex collection; merkle/fvc whitelists return null and
// their bids simply don't surface in the collection floor/bid join.
//
// WhitelistV2 layout (176 bytes) — voc offset VERIFIED empirically against known floor collections
// (49/49 clean matches had tag@143==1, voc pubkey @144..176; no other offset matched any collection):
//   … [143] voc Option tag (1 = Some) … [144..176] voc pubkey (the collection mint)
//
// Routing: lib/rpc-client (the indexer's Triton endpoint) — NOT Helius. voc is immutable → cache forever.
'use strict';

const bs58 = require('bs58').default ?? require('bs58');
const { rpcCall } = require('./rpc-client');

const WHITELIST_PROGRAM = 'TL1ST2iRBzuGTqLn1KXnGdSnEow62BzPnGiqyRXhWtW';
const VOC_TAG_OFFSET = 143;
const VOC_OFFSET     = 144;
const SYSTEM_ADDR    = '11111111111111111111111111111111';

function parseVoc(buf) {
  if (!buf || buf.length < VOC_OFFSET + 32) return null;
  if (buf[VOC_TAG_OFFSET] !== 1) return null;          // voc not set (merkle / fvc whitelist)
  const voc = bs58.encode(buf.slice(VOC_OFFSET, VOC_OFFSET + 32));
  return voc === SYSTEM_ADDR ? null : voc;
}

// Batched whitelist → voc resolver. Returns Map(whitelist → voc|null). null = no voc (cache it too).
async function resolveVocs(whitelists, { chunkSize = 100 } = {}) {
  const out = new Map();
  for (let i = 0; i < whitelists.length; i += chunkSize) {
    const slice = whitelists.slice(i, i + chunkSize);
    let res = null;
    try {
      res = await rpcCall('getMultipleAccounts', [slice, { encoding: 'base64', dataSlice: { offset: 0, length: 176 } }]);
    } catch { res = null; }
    const values = res?.value ?? [];
    slice.forEach((wl, j) => {
      const acc = values[j];
      out.set(wl, acc?.data?.[0] ? parseVoc(Buffer.from(acc.data[0], 'base64')) : null);
    });
  }
  return out;
}

module.exports = { resolveVocs, parseVoc, WHITELIST_PROGRAM };
