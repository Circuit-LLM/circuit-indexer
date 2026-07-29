// lib/metaplex.js — Resolve a regular-NFT mint → its verified collection key, on-chain.
//
// Reads the Metaplex Token Metadata account (a PDA of the mint) and parses out the
// `collection` field. Ported from circuit-node/sources/magiceden.js (_getMetaplexPda /
// _getCollectionKey), extended with a BATCHED getMultipleAccounts resolver for the
// NFT-listing reconciliation.
//
// Routing: uses lib/rpc-client (CIRCUIT_RPC_URL = the indexer's Triton endpoint) — NOT Helius.
// Collection membership is immutable, so callers cache the result forever.
//
// Compressed NFTs (cNFTs): their assetId is a merkle-tree leaf, NOT a mint account, so the
// derived metadata PDA does not exist → this returns null for them (they're skipped in Phase 0).
'use strict';

const bs58 = require('bs58').default ?? require('bs58');
const { PublicKey } = require('@solana/web3.js');
const { rpcCall } = require('./rpc-client');

const METAPLEX   = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s';
const META_SEED  = Buffer.from('metadata');
const META_PROG  = bs58.decode(METAPLEX);
const META_PUBKEY = new PublicKey(METAPLEX);

function metadataPda(mint) {
  try {
    const [pda] = PublicKey.findProgramAddressSync(
      [META_SEED, META_PROG, bs58.decode(mint)],
      META_PUBKEY
    );
    return pda.toBase58();
  } catch { return null; }
}

// Parse the verified-collection pubkey out of a raw Metaplex metadata account buffer.
// Metadata strings are "puffed" to fixed max lengths on-chain, so the pre-creators layout is fixed;
// creators/edition/token-standard are variable options walked sequentially. Returns null if the
// account has no collection set. (Layout mirrors circuit-node/sources/magiceden.js.)
function parseCollectionKey(buf) {
  try {
    if (!buf || buf.length < 360) return null;
    //   [0]key(1) [1:33]updateAuthority [33:65]mint [65:69]nameLen [69:101]name(32)
    //   [101:105]symLen [105:115]symbol(10) [115:119]uriLen [119:319]uri(200)
    //   [319:321]sellerFeeBps
    let off = 321;
    const creatorsPresent = buf[off++];
    if (creatorsPresent) {
      const creatorsLen = buf.readUInt32LE(off); off += 4;
      off += creatorsLen * (32 + 1 + 2); // pubkey + verified + share
    }
    off += 2; // primary_sale_happened(1) + is_mutable(1)
    const editionNoncePresent = buf[off++]; if (editionNoncePresent) off += 1;
    const tokenStdPresent      = buf[off++]; if (tokenStdPresent) off += 1;
    const collectionPresent    = buf[off++];
    if (!collectionPresent || buf.length < off + 33) return null;
    off += 1; // verified flag
    return bs58.encode(buf.subarray(off, off + 32));
  } catch { return null; }
}

// Batched mint → collection resolver. Returns a Map(mint → collectionKey|null).
// Derives each metadata PDA locally, then getMultipleAccounts in chunks (Triton, off-Helius).
// `null` for a mint means "no collection / not a regular NFT" — cache it too so we don't re-resolve.
async function resolveCollections(mints, { chunkSize = 100 } = {}) {
  const out = new Map();
  const pdas = mints.map(m => ({ mint: m, pda: metadataPda(m) }));

  for (let i = 0; i < pdas.length; i += chunkSize) {
    const slice = pdas.slice(i, i + chunkSize);
    const addrs = slice.map(s => s.pda).filter(Boolean);
    if (!addrs.length) { slice.forEach(s => out.set(s.mint, null)); continue; }
    let res = null;
    try {
      res = await rpcCall('getMultipleAccounts', [addrs, { encoding: 'base64' }]);
    } catch { res = null; }
    const values = res?.value ?? [];
    let vi = 0;
    for (const s of slice) {
      if (!s.pda) { out.set(s.mint, null); continue; }
      const acc = values[vi++];
      if (!acc?.data?.[0]) { out.set(s.mint, null); continue; }
      out.set(s.mint, parseCollectionKey(Buffer.from(acc.data[0], 'base64')));
    }
  }
  return out;
}

// Parse the collection's human name from its own metadata account. Metaplex "puffs" the name to
// MAX_NAME_LENGTH (32), length-prefixed at [65]; read that many bytes, strip padding.
function parseName(buf) {
  try {
    if (!buf || buf.length < 101) return null;
    const len = Math.min(buf.readUInt32LE(65), 32);
    const name = buf.slice(69, 69 + len).toString('utf8').replace(/\0/g, '').trim();
    return name || null;
  } catch { return null; }
}

// seller_fee_basis_points (royalty) — u16 LE at offset 319, right before the creators field.
function parseSellerFeeBps(buf) {
  try { return buf && buf.length >= 321 ? buf.readUInt16LE(319) : null; } catch { return null; }
}

// Batched metadata resolver → Map(mint → { name, royaltyBps }). One fetch covers both (name @69,
// royalty @319). Off-Helius (Triton); callers cache the result forever.
async function resolveMeta(mints, { chunkSize = 100 } = {}) {
  const out = new Map();
  const pdas = mints.map((m) => ({ mint: m, pda: metadataPda(m) }));
  for (let i = 0; i < pdas.length; i += chunkSize) {
    const slice = pdas.slice(i, i + chunkSize);
    const addrs = slice.map((s) => s.pda).filter(Boolean);
    if (!addrs.length) { slice.forEach((s) => out.set(s.mint, { name: null, royaltyBps: null })); continue; }
    let res = null;
    try { res = await rpcCall('getMultipleAccounts', [addrs, { encoding: 'base64', dataSlice: { offset: 0, length: 321 } }]); }
    catch { res = null; }
    const values = res?.value ?? [];
    let vi = 0;
    for (const s of slice) {
      if (!s.pda) { out.set(s.mint, { name: null, royaltyBps: null }); continue; }
      const acc = values[vi++];
      const buf = acc?.data?.[0] ? Buffer.from(acc.data[0], 'base64') : null;
      out.set(s.mint, { name: parseName(buf), royaltyBps: parseSellerFeeBps(buf) });
    }
  }
  return out;
}

module.exports = { metadataPda, parseCollectionKey, parseName, parseSellerFeeBps, resolveCollections, resolveMeta, METAPLEX };
