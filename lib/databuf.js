'use strict';
// Normalize Geyser account data to a Buffer WITHOUT a base58 round-trip.
//
// The gRPC consumer now passes raw account bytes (Uint8Array/Buffer) straight through —
// base58-encoding the full blob in the consumer just to base58-decode it in every parser was
// the indexer's dominant CPU cost (O(n^2) base58 on 752–1544 byte pool accounts, every event).
//
// The file-stream consumer (replay from .jsonl) still stores data as a base58 string, so we
// accept that too for backward compatibility. base58 is now only ever used for the few 32-byte
// pubkey fields a parser actually extracts — never the whole account blob.
let _bs58;
function toBuf(data) {
  if (Buffer.isBuffer(data)) return data;
  if (data instanceof Uint8Array) return Buffer.from(data);
  if (typeof data === 'string') {
    if (!_bs58) _bs58 = require('bs58').default ?? require('bs58');
    return Buffer.from(_bs58.decode(data));
  }
  return Buffer.alloc(0);
}
module.exports = { toBuf };
