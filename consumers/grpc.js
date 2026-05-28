// consumers/grpc.js — yellowstone-grpc client for Geyser streaming.
//
// Connects to a managed Geyser gRPC endpoint (Triton, Helius, QuickNode).
// This is the PRODUCTION alternative to circuit-geyser .so plugin:
//   - No need to run your own validator during development
//   - Same data stream — Triton/Helius run the plugin, you consume via gRPC
//   - Swap in your own validator endpoint when you run one
//
// Environment variables:
//   GEYSER_ENDPOINT=https://your-endpoint.rpc.triton.one:10000
//   GEYSER_TOKEN=your-access-token
//
// Free testnet gRPC endpoints:
//   Triton One devnet: https://api.rpcpool.com  (contact them for token)
//   Solana public gRPC: limited, check docs
//
// Install: npm install @triton-one/yellowstone-grpc
// Docs: https://github.com/rpcpool/yellowstone-grpc
'use strict';

const Logger = require('../lib/logger');

// Well-known program IDs to subscribe to
const WATCHED_PROGRAMS = [
  '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // Raydium AMM v4
  'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK', // Raydium CLMM
  'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',  // Orca Whirlpools
  'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',  // Meteora DLMM
  'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',  // Token Program
  'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',  // Token-2022
];

class GrpcConsumer {
  /**
   * @param {function} onEvent - called with normalized Geyser event
   */
  constructor(onEvent) {
    this._onEvent  = onEvent;
    this._client   = null;
    this._stream   = null;
    this._running  = false;
    this._stats    = { events: 0, reconnects: 0, errors: 0, lastEventTs: null };
  }

  async start() {
    this._running = true;

    let Client;
    try {
      ({ default: Client } = await import('@triton-one/yellowstone-grpc'));
    } catch {
      throw new Error(
        'yellowstone-grpc not installed. Run: npm install @triton-one/yellowstone-grpc\n' +
        'Docs: https://github.com/rpcpool/yellowstone-grpc'
      );
    }

    const endpoint = process.env.GEYSER_ENDPOINT;
    const token    = process.env.GEYSER_TOKEN;

    if (!endpoint) {
      throw new Error('GEYSER_ENDPOINT env var required (e.g. https://your-endpoint:10000)');
    }

    Logger.info('GrpcConsumer: connecting', { endpoint });

    this._client = new Client(endpoint, token, {
      // gRPC channel options
      'grpc.max_receive_message_length': 64 * 1024 * 1024, // 64 MB
    });

    await this._subscribe();
  }

  async _subscribe() {
    if (!this._running) return;

    try {
      const stream = await this._client.subscribe();
      this._stream = stream;

      stream.on('data', (data) => {
        this._stats.lastEventTs = Date.now();
        try {
          this._handleGrpcEvent(data);
        } catch (e) {
          this._stats.errors++;
          Logger.error('GrpcConsumer: event handler error', { error: e.message });
        }
      });

      stream.on('error', (e) => {
        Logger.warn('GrpcConsumer: stream error', { error: e.message });
        this._reconnect();
      });

      stream.on('end', () => {
        Logger.warn('GrpcConsumer: stream ended');
        this._reconnect();
      });

      // Send subscription request
      const subscribeRequest = this._buildSubscribeRequest();
      await new Promise((resolve, reject) => {
        stream.write(subscribeRequest, (err) => err ? reject(err) : resolve());
      });

      Logger.info('GrpcConsumer: subscribed', {
        programs: WATCHED_PROGRAMS.length,
      });

    } catch (e) {
      Logger.error('GrpcConsumer: subscription failed', { error: e.message });
      this._reconnect();
    }
  }

  _buildSubscribeRequest() {
    // Subscribe to account updates for watched programs + all transactions
    // yellowstone-grpc subscription format
    return {
      accounts: {
        circuit: {
          account: [],
          owner:   WATCHED_PROGRAMS,
          filters: [],
        },
      },
      transactions: {
        circuit: {
          vote:              false, // skip vote transactions
          failed:            false, // skip failed transactions
          signature:         null,
          accountInclude:    WATCHED_PROGRAMS,
          accountExclude:    [],
          accountRequired:   [],
        },
      },
      slots:  { circuit: {} },
      blocks: {},
      blocksMeta: {},
      commitment: 1, // Confirmed
      accountsDataSlice: [],
      ping: null,
    };
  }

  _handleGrpcEvent(data) {
    this._stats.events++;

    // yellowstone-grpc uses oneof field in the protobuf message
    if (data.account) {
      const acc = data.account.account;
      if (!acc) return;

      const event = {
        type:       'account',
        pubkey:     _tob58(acc.pubkey),
        owner:      _tob58(acc.owner),
        lamports:   Number(acc.lamports),
        data:       _tob58(acc.data),
        executable: acc.executable,
        rentEpoch:  Number(acc.rentEpoch),
        slot:       Number(data.account.slot),
        ts:         Date.now(),
      };
      this._onEvent(event);

    } else if (data.transaction) {
      const tx = data.transaction.transaction;
      if (!tx) return;

      const accounts = (tx.transaction?.message?.accountKeys ?? []).map(_tob58);
      const event = {
        type:      'transaction',
        signature: _tob58(data.transaction.signature),
        slot:      Number(data.transaction.slot),
        accounts,
        fee:       Number(tx.meta?.fee ?? 0),
        success:   !tx.meta?.err,
        ts:        Date.now(),
      };
      this._onEvent(event);

    } else if (data.slot) {
      this._onEvent({
        type:   'slot',
        slot:   Number(data.slot.slot),
        status: 'confirmed',
        ts:     Date.now(),
      });

    } else if (data.ping) {
      // Keepalive ping — respond with pong
      if (this._stream) {
        this._stream.write({ pong: { id: data.ping.id } }).catch(() => {});
      }
    }
  }

  _reconnect() {
    if (!this._running) return;
    this._stats.reconnects++;
    const delay = Math.min(5000 * this._stats.reconnects, 60_000);
    Logger.info('GrpcConsumer: reconnecting', { delay, attempt: this._stats.reconnects });
    setTimeout(() => this._subscribe(), delay);
  }

  stop() {
    this._running = false;
    if (this._stream) {
      this._stream.end();
      this._stream = null;
    }
  }

  stats() { return { ...this._stats }; }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

let bs58 = null;
function _tob58(bytes) {
  if (!bytes || !bytes.length) return '';
  if (!bs58) bs58 = require('bs58').default ?? require('bs58');
  return bs58.encode(Buffer.from(bytes));
}

module.exports = { GrpcConsumer, WATCHED_PROGRAMS };
