// lib/rpc-client.js — Minimal JSON-RPC client for Solana RPC.
// Used by the indexer to fetch supplemental on-chain data (e.g. mint decimals)
// that isn't delivered via the gRPC stream.
'use strict';

const Logger = require('./logger');

const RPC_URL = process.env.CIRCUIT_RPC_URL || process.env.HELIUS_RPC_URL || 'https://api.mainnet-beta.solana.com';

let _reqId = 1;

async function rpcCall(method, params = []) {
  const body = JSON.stringify({ jsonrpc: '2.0', id: _reqId++, method, params });
  try {
    const resp = await fetch(RPC_URL, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      signal:  AbortSignal.timeout(8000),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const json = await resp.json();
    if (json.error) throw new Error(json.error.message ?? JSON.stringify(json.error));
    return json.result;
  } catch (e) {
    Logger.debug('rpc-client: call failed', { method, error: e.message });
    return null;
  }
}

module.exports = { rpcCall };
