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

// Well-known program IDs to subscribe to.
// NOTE: Raydium AMM v4 (675kPX9...) removed — vault balances aren't in account
// data so price is always null, producing only garbage pool records in Redis.
const WATCHED_PROGRAMS = [
  'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK', // Raydium CLMM
  'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C', // Raydium CPMM
  'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',  // Orca Whirlpools
  // Meteora DLMM (LBUZKh…) removed — there is NO meteora parser, so 100% of its events were
  // received and discarded: ~31% of the entire firehose (85MB/45s) of pure waste. Re-add this
  // line ONLY together with a parsers/meteora.js if we want Meteora market coverage.
  '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',  // Pump.fun bonding curve
  'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',  // PumpSwap AMM
  'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',  // Token Program
  'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',  // Token-2022
];

// Named program owners for the multi-stream sliced mode (CIRCUIT_STREAM_SLICING=1).
const PROG = {
  CLMM:  'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
  CPMM:  'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',
  ORCA:  'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
  PUMP:  '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',
  PSWAP: 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',
  TOK:   'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
  TOK22: 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',
};

class GrpcConsumer {
  /**
   * @param {function} onEvent - called with normalized Geyser event
   */
  constructor(onEvent, opts = {}) {
    this._onEvent       = onEvent;
    this._client        = null;
    this._stream        = null;
    this._running       = false;
    this._reconnecting  = false;  // guard: prevents concurrent reconnect timers
    this._reconnectTimer = null;  // current pending reconnect timer
    // Narrowed-subscription mode (CIRCUIT_NARROW=1): subscribe to the specific registered pool VAULTS
    // + size-filtered mints, instead of every SPL token account. getVaults() supplies the live vault
    // pubkey list and _maybeResubscribe() re-sends the request as it grows. No-op when the flag is off
    // (prod path unchanged), so this can ship dark and be validated by a shadow consumer first.
    this._getVaults      = opts.getVaults || null;
    this._narrow         = process.env.CIRCUIT_NARROW === '1' && !!this._getVaults;
    this._lastVaultCount = -1;
    this._resubTimer     = null;
    this._stats    = { events: 0, reconnects: 0, errors: 0, lastEventTs: null };
  }

  async start() {
    this._running = true;

    let Client;
    try {
      const ygModule = require('@triton-one/yellowstone-grpc');
      Client = ygModule.default ?? ygModule;
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
      'grpc.default_compression_algorithm': 2, // gzip — request a compressed stream to cut egress bytes (Triton bills wire volume)
    });

    // Multi-stream sliced mode: partition the subscription into per-slice-tier streams so each
    // account type streams at only the prefix its parser reads. Requires narrow mode (vault list).
    // Default (flag off) → the original single stream, unchanged.
    if (process.env.CIRCUIT_STREAM_SLICING === '1' && this._narrow) {
      await this._startMulti();
    } else {
      await this._subscribe();
    }
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

      // Successful subscription — reset reconnect counter so backoff starts fresh
      this._stats.reconnects = 0;
      Logger.info('GrpcConsumer: subscribed', {
        programs: WATCHED_PROGRAMS.length, narrow: this._narrow,
      });
      if (this._narrow && !this._resubTimer) {
        this._resubTimer = setInterval(() => this._maybeResubscribe(), 8000);
        if (this._resubTimer.unref) this._resubTimer.unref();
      }

    } catch (e) {
      Logger.error('GrpcConsumer: subscription failed', { error: e.message });
      this._reconnect();
    }
  }

  _buildSubscribeRequest() {
    const { SubscribeRequest } = require('@triton-one/yellowstone-grpc/dist/grpc/geyser');
    const txWatch = (process.env.CIRCUIT_TX_WATCHLIST || '').split(',').map(s => s.trim()).filter(Boolean);

    // Pool-state subscriptions (always on). dataSize filters drop the large tick-array / observation
    // accounts these programs also own (the parsers discard those anyway). Sizes verified on-chain
    // (CLMM PoolState 1544, Orca Whirlpool 653, PumpSwap pool 301).
    const accounts = {
      clmm:     { account: [], owner: ['CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK'], filters: [{ datasize: 1544 }] },
      orca:     { account: [], owner: ['whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'],  filters: [{ datasize: 653 }] },
      pumpswap: { account: [], owner: ['pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'], filters: [{ datasize: 301 }] },
    };
    if (this._narrow) {
      // CPMM + Pump.fun pool states stay (they populate the vault registry). Replace the unfiltered
      // Token/Token-2022 owner with: (a) the specific registered VAULTS — exactly what produces prices
      // today — and (b) mints only (82-byte) for metadata. This is the firehose cut. The vault list is
      // grown by _maybeResubscribe() as new pools register their vaults.
      accounts.cpmmpf = { account: [], owner: ['CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'], filters: [] };
      accounts.mints  = { account: [], owner: ['TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'], filters: [{ datasize: 82 }] };
      const vaults = (this._getVaults && this._getVaults()) || [];
      this._lastVaultCount = vaults.length;
      if (vaults.length) accounts.vaults = { account: vaults, owner: [], filters: [] };
    } else {
      // CPMM pool config + Pump.fun bonding curves are small (left unfiltered). Token/Token-2022 are
      // kept UNFILTERED here for CPMM/PumpSwap vault balances (the firehose narrowed away above when
      // CIRCUIT_NARROW=1).
      accounts.rest = { account: [], owner: ['CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C', '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P', 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'], filters: [] };
    }

    return SubscribeRequest.fromPartial({
      accounts,
      // Scoped transaction stream (surgical 1d-c reversal). The full firehose was dropped because
      // transactions were ~76% of it (~$13.9/day), but deriving candles from vault-balance deltas
      // undercounts trades that share a slot (Geyser emits net per-slot state), so low-volume tokens
      // look sparse. When CIRCUIT_TX_WATCHLIST is set we re-subscribe ONLY to transactions touching
      // those pool accounts — restoring per-swap candle fidelity (handleTransaction) for them at a
      // few MB/day, not the whole firehose. Empty list => no transaction stream (vault-delta candles
      // for everything, the cheap default). Watchlist pools skip the vault-delta candle tick in
      // indexer.js so the two paths never double-count.
      transactions: txWatch.length ? {
        circuit: { vote: false, failed: false, accountInclude: txWatch, accountExclude: [], accountRequired: [] },
      } : {},
      slots:             { circuit: {} },
      blocks:            {},
      blocksMeta:        {},
      commitment:        1, // Confirmed
      // Egress optimization: stream only the prefix each parser reads (global per stream).
      // CIRCUIT_ACCT_SLICE=<bytes> (e.g. 333) trims CLMM/CPMM/Orca to their read length; smaller
      // accounts (vault/pump.fun/PumpSwap/mint) are already ≤ the slice, so untouched. Unset/0 = full
      // account (current behavior). Requires the CLMM (273) + Orca (213) guard relaxations in parsers/.
      accountsDataSlice: this._acctSlice(),
    });
  }

  // Global accountsDataSlice from env — [] when unset (byte-for-byte current behavior).
  _acctSlice() {
    const n = Number(process.env.CIRCUIT_ACCT_SLICE);
    return n > 0 ? [{ offset: '0', length: String(n) }] : [];
  }

  // ── Multi-stream sliced mode (CIRCUIT_STREAM_SLICING=1) ──────────────────────────────────────
  // Triton's accountsDataSlice DROPS any account shorter than the slice, so a single global slice is
  // impossible. Instead split into per-tier streams, each sliced to the max byte its parsers read
  // (which is ≤ its smallest account): A=vaults+pump.fun@72, B=PumpSwap+Orca@215, C=CLMM+CPMM@333
  // (+slots+tx), D=mints unsliced. Every stream reconnects independently and feeds one _handleGrpcEvent.
  _buildReq(accounts, slice, { slots = false, tx = false } = {}) {
    const { SubscribeRequest } = require('@triton-one/yellowstone-grpc/dist/grpc/geyser');
    const txWatch = (process.env.CIRCUIT_TX_WATCHLIST || '').split(',').map(s => s.trim()).filter(Boolean);
    return SubscribeRequest.fromPartial({
      accounts,
      transactions: (tx && txWatch.length)
        ? { circuit: { vote: false, failed: false, accountInclude: txWatch, accountExclude: [], accountRequired: [] } }
        : {},
      slots:      slots ? { circuit: {} } : {},
      blocks:     {},
      blocksMeta: {},
      commitment: 1,
      accountsDataSlice: slice > 0 ? [{ offset: '0', length: String(slice) }] : [],
    });
  }

  _streamSpecs() {
    const vaults = () => (this._getVaults && this._getVaults()) || [];
    return [
      { name: 'A-small', slice: 72,  resub: true,  build: () => this._buildReq({
          vaults:  { account: vaults(), owner: [],           filters: [] },
          pumpfun: { account: [],       owner: [PROG.PUMP],  filters: [] },
        }, 72) },
      { name: 'B-mid',   slice: 215, resub: false, build: () => this._buildReq({
          pumpswap: { account: [], owner: [PROG.PSWAP], filters: [{ datasize: 301 }] },
          orca:     { account: [], owner: [PROG.ORCA],  filters: [{ datasize: 653 }] },
        }, 215) },
      { name: 'C-pool',  slice: 333, resub: false, build: () => this._buildReq({
          clmm: { account: [], owner: [PROG.CLMM], filters: [{ datasize: 1544 }] },
          cpmm: { account: [], owner: [PROG.CPMM], filters: [] },
        }, 333, { slots: true, tx: true }) },   // slots + tx-watchlist ride here (slice ignores them)
      { name: 'D-mints', slice: 0,   resub: false, build: () => this._buildReq({
          mints: { account: [], owner: [PROG.TOK, PROG.TOK22], filters: [{ datasize: 82 }] },
        }, 0) },
    ];
  }

  async _startMulti() {
    this._handles = this._streamSpecs().map(spec => ({ spec, stream: null, reconnecting: false, reconnectTimer: null, resubTimer: null, attempt: 0 }));
    for (const h of this._handles) await this._subscribeStream(h);
    Logger.info('GrpcConsumer: multi-stream sliced mode', { streams: this._handles.map(h => `${h.spec.name}@${h.spec.slice || 'full'}`) });
  }

  async _subscribeStream(h) {
    if (!this._running) return;
    try {
      const stream = await this._client.subscribe();
      h.stream = stream;
      stream.on('data', (data) => {
        this._stats.lastEventTs = Date.now();
        try { this._handleGrpcEvent(data, stream); }
        catch (e) { this._stats.errors++; Logger.error('GrpcConsumer: event handler error', { stream: h.spec.name, error: e.message }); }
      });
      stream.on('error', (e) => { Logger.warn('GrpcConsumer: stream error', { stream: h.spec.name, error: e.message }); this._reconnectStream(h); });
      stream.on('end',   ()  => { Logger.warn('GrpcConsumer: stream ended', { stream: h.spec.name }); this._reconnectStream(h); });
      await new Promise((res, rej) => stream.write(h.spec.build(), (err) => err ? rej(err) : res()));
      h.attempt = 0;
      if (h.spec.resub) {
        this._lastVaultCount = ((this._getVaults && this._getVaults()) || []).length;
        if (!h.resubTimer) {
          h.resubTimer = setInterval(() => this._maybeResubStream(h), 8000);
          if (h.resubTimer.unref) h.resubTimer.unref();
        }
      }
      Logger.info('GrpcConsumer: stream subscribed', { stream: h.spec.name, slice: h.spec.slice || 'full' });
    } catch (e) {
      Logger.error('GrpcConsumer: stream subscribe failed', { stream: h.spec.name, error: e.message });
      this._reconnectStream(h);
    }
  }

  _reconnectStream(h) {
    if (!this._running || h.reconnecting) return;   // per-stream single-timer guard
    h.reconnecting = true;
    if (h.stream) { try { h.stream.cancel?.(); h.stream.end?.(); } catch {} h.stream = null; }
    h.attempt = (h.attempt || 0) + 1;
    this._stats.reconnects++;
    const delay = Math.min(1000 * Math.pow(2, Math.min(h.attempt - 1, 6)), 60_000);
    Logger.info('GrpcConsumer: stream reconnecting', { stream: h.spec.name, delay, attempt: h.attempt });
    h.reconnectTimer = setTimeout(() => { h.reconnecting = false; h.reconnectTimer = null; this._subscribeStream(h); }, delay);
  }

  _maybeResubStream(h) {
    if (!this._running || !h.stream) return;
    const n = ((this._getVaults && this._getVaults()) || []).length;
    if (n === this._lastVaultCount) return;
    this._lastVaultCount = n;
    try {
      h.stream.write(h.spec.build(), (err) => {
        if (err) Logger.warn('GrpcConsumer: stream re-subscribe failed', { stream: h.spec.name, error: err.message });
        else     Logger.info('GrpcConsumer: stream re-subscribed', { stream: h.spec.name, vaults: n });
      });
    } catch (e) { Logger.warn('GrpcConsumer: stream re-subscribe threw', { stream: h.spec.name, error: e.message }); }
  }

  // Re-send the subscription when the vault registry has grown (narrow mode only). yellowstone
  // replaces the active subscription on each SubscribeRequest, so this just widens the vault set.
  _maybeResubscribe() {
    if (!this._running || !this._stream || !this._narrow) return;
    const n = ((this._getVaults && this._getVaults()) || []).length;
    if (n === this._lastVaultCount) return; // nothing new registered since last subscribe
    try {
      this._stream.write(this._buildSubscribeRequest(), (err) => {
        if (err) Logger.warn('GrpcConsumer: vault re-subscribe failed', { error: err.message });
        else     Logger.info('GrpcConsumer: re-subscribed', { vaults: n });
      });
    } catch (e) {
      Logger.warn('GrpcConsumer: vault re-subscribe threw', { error: e.message });
    }
  }

  _handleGrpcEvent(data, stream = null) {
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
        data:       acc.data,   // raw bytes (Uint8Array) — parsers normalize via toBuf(); no base58 round-trip
        executable: acc.executable,
        rentEpoch:  Number(acc.rentEpoch),
        slot:       Number(data.account.slot),
        ts:         Date.now(),
      };
      this._onEvent(event);

    } else if (data.transaction) {
      const tx = data.transaction.transaction;
      if (!tx) return;

      // Full account list in canonical order: static keys, then address-lookup-table loaded
      // writable, then loaded readonly. PumpSwap swaps are versioned txns using lookup tables, so
      // the vaults live in the loaded addresses; token-balance accountIndex references this full
      // list. Omitting loaded addresses (the prior behaviour) mis-resolved the vault and dropped
      // most swaps — only legacy transactions matched.
      const accounts = [
        ...(tx.transaction?.message?.accountKeys ?? []),
        ...(tx.meta?.loadedWritableAddresses ?? []),
        ...(tx.meta?.loadedReadonlyAddresses ?? []),
      ].map(_tob58);
      const preTokenBalances  = (tx.meta?.preTokenBalances  ?? []).map(b => ({
        accountIndex: b.accountIndex,
        mint:         b.mint,
        owner:        b.owner,
        amount:       b.uiTokenAmount?.amount ?? '0',
      }));
      const postTokenBalances = (tx.meta?.postTokenBalances ?? []).map(b => ({
        accountIndex: b.accountIndex,
        mint:         b.mint,
        owner:        b.owner,
        amount:       b.uiTokenAmount?.amount ?? '0',
      }));
      const event = {
        type:      'transaction',
        signature: _tob58(data.transaction.signature),
        slot:      Number(data.transaction.slot),
        accounts,
        preTokenBalances,
        postTokenBalances,
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
      // Keepalive ping — respond with pong on the SAME stream that sent it (multi-stream: `stream`;
      // single-stream: this._stream). write() takes a callback, not a Promise.
      const s = stream || this._stream;
      if (s) {
        const { SubscribeRequest } = require('@triton-one/yellowstone-grpc/dist/grpc/geyser');
        const pong = SubscribeRequest.fromPartial({ ping: { id: data.ping.id } });
        s.write(pong, (err) => {
          if (err) Logger.warn('GrpcConsumer: pong write failed', { error: err.message });
        });
      }
    }
  }

  _reconnect() {
    if (!this._running) return;

    // Guard: only one reconnect timer at a time.
    // Without this, each "error" + "end" event pair (and any catch block)
    // spawns its own setTimeout, causing an exponential timer storm that
    // fills the event loop and leaks hundreds of MB of closure memory.
    if (this._reconnecting) return;
    this._reconnecting = true;

    // Close current stream cleanly before scheduling reconnect
    if (this._stream) {
      try { this._stream.cancel?.(); this._stream.end?.(); } catch {}
      this._stream = null;
    }

    this._stats.reconnects++;
    // Exponential backoff capped at 60s. Use a separate attempt counter
    // that resets to 0 on successful connection so backoff doesn't grow forever.
    const attempt = this._stats.reconnects;
    const delay   = Math.min(1000 * Math.pow(2, Math.min(attempt - 1, 6)), 60_000);
    Logger.info('GrpcConsumer: reconnecting', { delay, attempt });

    this._reconnectTimer = setTimeout(() => {
      this._reconnecting  = false;
      this._reconnectTimer = null;
      this._subscribe();
    }, delay);
  }

  stop() {
    this._running = false;
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    if (this._resubTimer) {
      clearInterval(this._resubTimer);
      this._resubTimer = null;
    }
    if (this._stream) {
      try { this._stream.cancel?.(); this._stream.end?.(); } catch {}
      this._stream = null;
    }
    // Multi-stream mode: tear down every per-stream handle (timers + streams).
    if (this._handles) {
      for (const h of this._handles) {
        if (h.reconnectTimer) clearTimeout(h.reconnectTimer);
        if (h.resubTimer)     clearInterval(h.resubTimer);
        if (h.stream) { try { h.stream.cancel?.(); h.stream.end?.(); } catch {} h.stream = null; }
      }
      this._handles = null;
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
