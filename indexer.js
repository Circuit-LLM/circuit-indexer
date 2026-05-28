// indexer.js — Circuit Indexer main process.
//
// Consumes Geyser event stream → parses pool/token state → writes to Redis + Postgres.
//
// Usage:
//   node indexer.js                              # defaults: file consumer, /tmp/circuit-geyser.jsonl
//   node indexer.js --consumer=stdin             # pipe from solana-test-validator stdout
//   node indexer.js --consumer=grpc              # yellowstone-grpc (managed geyser endpoint)
//   node indexer.js --consumer=file --file=PATH  # tail a specific file
//
// Environment:
//   REDIS_URL=redis://127.0.0.1:6379
//   DATABASE_URL=postgresql://localhost/circuit_index
//   GEYSER_ENDPOINT=https://your-grpc-endpoint.com:10000  # for grpc consumer
//   GEYSER_TOKEN=your-access-token                        # for grpc consumer
'use strict';

process.stdout.on('error', err => { if (err.code !== 'EPIPE') throw err; });
process.stderr.on('error', err => { if (err.code !== 'EPIPE') throw err; });

const Logger     = require('./lib/logger');
const { OHLCVAggregator } = require('./lib/ohlcv');
const raydium    = require('./parsers/raydium');
const orca       = require('./parsers/orca');
const token      = require('./parsers/token');
const redis      = require('./writers/redis');
const postgres   = require('./writers/postgres');

// ── CLI args ──────────────────────────────────────────────────────────────────

const args = Object.fromEntries(
  process.argv.slice(2)
    .filter(a => a.startsWith('--'))
    .map(a => {
      const [k, v] = a.slice(2).split('=');
      return [k, v ?? 'true'];
    })
);

const CONSUMER   = args.consumer || 'file';
const FILE_PATH  = args.file     || process.env.GEYSER_FILE || '/tmp/circuit-geyser.jsonl';

// ── Stats ─────────────────────────────────────────────────────────────────────

const stats = {
  events:       0,
  accounts:     0,
  transactions: 0,
  slots:        0,
  pools:        0,
  newTokens:    0,
  candles:      0,
  errors:       0,
  startedAt:    Date.now(),
};

// ── OHLCV aggregator ──────────────────────────────────────────────────────────

const ohlcv = new OHLCVAggregator(async (candle) => {
  stats.candles++;
  try {
    await postgres.writeCandle(candle);
  } catch (e) {
    Logger.error('Failed to write candle', { error: e.message });
  }
});

// ── SOL/USDC mint (for price denominator) ────────────────────────────────────
const SOL_MINT  = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

// In-memory decimals map (populated as we see mint accounts)
const decimalsMap = new Map([
  [SOL_MINT,  9],
  [USDC_MINT, 6],
]);

// ── Event handler ─────────────────────────────────────────────────────────────

async function handleEvent(event) {
  stats.events++;
  try {
    switch (event.type) {
      case 'account':   await handleAccount(event);     break;
      case 'transaction': await handleTransaction(event); break;
      case 'slot':      stats.slots++;                  break;
    }
  } catch (e) {
    stats.errors++;
    Logger.error('Event handler error', { type: event.type, error: e.message });
  }
}

async function handleAccount(event) {
  stats.accounts++;

  // ── Try Raydium pool ──────────────────────────────────────────────────────
  const poolR = raydium.processAccountEvent(event);
  if (poolR) {
    stats.pools++;
    const mint = poolR.coinMint || poolR.mint0;
    const pc   = poolR.pcMint   || poolR.mint1;

    if (poolR.price !== null && poolR.price !== undefined) {
      // Convert to USD if denominator is SOL (we'd need SOL/USD for that)
      // For now store price as-is (in pc units)
      await redis.writePrice(mint, poolR.price, poolR.type);
      await redis.writePool(event.pubkey, poolR);
      ohlcv.tick(mint, poolR.price, 0, event.ts);
    }
    await postgres.writePool({ ...poolR, program: event.owner, slot: event.slot });
    return;
  }

  // ── Try Orca Whirlpool ────────────────────────────────────────────────────
  const poolO = orca.processAccountEvent(event);
  if (poolO) {
    stats.pools++;
    // Try to fill in decimals from our map
    if (poolO.mintA && decimalsMap.has(poolO.mintA)) poolO.decimalsA = decimalsMap.get(poolO.mintA);
    if (poolO.mintB && decimalsMap.has(poolO.mintB)) poolO.decimalsB = decimalsMap.get(poolO.mintB);

    if (poolO.decimalsA !== null && poolO.decimalsB !== null && poolO.sqrtPriceX64) {
      const { whirlpoolPrice } = require('./parsers/orca');
      poolO.price = whirlpoolPrice(BigInt(poolO.sqrtPriceX64), poolO.decimalsA, poolO.decimalsB);
    }

    if (poolO.price !== null && poolO.price !== undefined) {
      await redis.writePrice(poolO.mintA, poolO.price, 'orca-whirlpool');
      await redis.writePool(event.pubkey, poolO);
      ohlcv.tick(poolO.mintA, poolO.price, 0, event.ts);
    }
    await postgres.writePool({ ...poolO, program: event.owner, slot: event.slot });
    return;
  }

  // ── Try token mint ────────────────────────────────────────────────────────
  const mintInfo = token.processAccountEvent(event);
  if (mintInfo) {
    // Cache decimals for pool price calculations
    if (mintInfo.decimals !== undefined) {
      decimalsMap.set(mintInfo.mint, mintInfo.decimals);
    }
    stats.newTokens++;
    await redis.writeMint(mintInfo.mint, mintInfo);
    await postgres.writeToken({ ...mintInfo, slot: event.slot });
  }
}

async function handleTransaction(event) {
  stats.transactions++;
  // Transaction volume tracking: increment trending score for mints in tx
  // We'll do a simple heuristic — count the tx as volume for any watched pool
  // Real volume needs pre/post token balance parsing (done in Phase 2)
}

// ── Consumer setup ────────────────────────────────────────────────────────────

function startConsumer() {
  switch (CONSUMER) {
    case 'stdin': {
      const { StdinConsumer } = require('./consumers/file-stream');
      const c = new StdinConsumer(handleEvent);
      c.start();
      Logger.info('Indexer ready', { consumer: 'stdin' });
      break;
    }
    case 'grpc': {
      const { GrpcConsumer } = require('./consumers/grpc');
      const c = new GrpcConsumer(handleEvent);
      c.start().catch(e => {
        Logger.error('gRPC consumer failed to start', { error: e.message });
        process.exit(1);
      });
      Logger.info('Indexer ready', { consumer: 'grpc' });
      break;
    }
    case 'file':
    default: {
      const { FileStreamConsumer } = require('./consumers/file-stream');
      const c = new FileStreamConsumer(FILE_PATH, handleEvent);
      c.start();
      Logger.info('Indexer ready', { consumer: 'file', file: FILE_PATH });
      break;
    }
  }
}

// ── Stats reporting ───────────────────────────────────────────────────────────

setInterval(() => {
  const upMs   = Date.now() - stats.startedAt;
  const upMins = (upMs / 60000).toFixed(1);
  Logger.info('Indexer stats', {
    uptime:   `${upMins}m`,
    events:   stats.events,
    accounts: stats.accounts,
    txns:     stats.transactions,
    slots:    stats.slots,
    pools:    stats.pools,
    tokens:   stats.newTokens,
    candles:  stats.candles,
    errors:   stats.errors,
    eps:      (stats.events / (upMs / 1000)).toFixed(1),
  });
}, 30_000);

// ── Shutdown ──────────────────────────────────────────────────────────────────

async function shutdown(signal) {
  Logger.info(`${signal} — shutting down`);
  ohlcv.flush();
  await redis.disconnect().catch(() => {});
  await postgres.disconnect().catch(() => {});
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));

// ── Start ─────────────────────────────────────────────────────────────────────

Logger.info('circuit-indexer starting', { consumer: CONSUMER });
startConsumer();
