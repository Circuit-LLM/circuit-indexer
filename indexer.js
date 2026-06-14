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
const bs58       = require('bs58').default ?? require('bs58');
const raydium    = require('./parsers/raydium');
const orca       = require('./parsers/orca');
const cpmm       = require('./parsers/cpmm');
const pumpswap   = require('./parsers/pumpswap');
const pumpfun    = require('./parsers/pumpfun');
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
  // Also persist to Redis ring buffer — readable by circuit-price-feed without Postgres
  redis.writeCandleBuffer(candle).catch(() => {});
});

// Evict OHLCV buckets for tokens that stopped trading > 2h ago.
// Prevents unbounded memory growth from one-time token events.
setInterval(() => {
  const evicted = ohlcv.evictStale(2 * 3_600_000);
  if (evicted > 0) Logger.info('OHLCV: evicted stale buckets', { count: evicted, remaining: ohlcv.bucketCount() });
}, 10 * 60_000).unref();

// ── SOL/USDC mint (for price denominator) ────────────────────────────────────
const SOL_MINT  = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const USDT_MINT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB';
const STABLE_MINTS = new Set([USDC_MINT, USDT_MINT]);

// Only write a USD price to Redis when the pool quote is a stable (USDC/USDT).
// For non-stable quotes (SOL/BONK, SOL/JUP etc.) the pool price is in quote units
// not USD — storing it as priceUsd would corrupt downstream consumers.
function _isUsdQuote(quoteMint) {
  return STABLE_MINTS.has(quoteMint);
}

// In-memory decimals map (populated as we see mint accounts)
const decimalsMap = new Map([
  [SOL_MINT,  9],
  [USDC_MINT, 6],
  ['Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 6],  // USDT
  ['DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', 5],  // BONK
  ['JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN',  6],  // JUP
]);

// Wrap redis.writePriceSol to also append to the price-history ring buffer.
// Keeps all call sites clean — no change needed elsewhere in this file.
const _writePriceSol = redis.writePriceSol.bind(redis);
redis.writePriceSol = async function(mint, priceSol, source, extraFields = {}) {
  await _writePriceSol(mint, priceSol, source, extraFields);
  redis.appendPriceHistory(mint, priceSol, Date.now()).catch(() => {});
};

// CPMM vault registry: vault_pubkey → { poolAccount, isVault0, mint0, mint1, dec0, dec1 }
// Populated when we see CPMM pool state accounts; used to compute price on vault updates.
const vaultRegistry = new Map();

// Pending decimals fetches to avoid duplicate concurrent lookups
const _pendingDecimalsFetch = new Set();

/**
 * Fetch decimals for unknown mints via RPC getMultipleAccounts.
 * Populates decimalsMap and vaultRegistry entries.
 */
async function _fetchAndCacheDecimals(mints) {
  const unknown = mints.filter(m => m && !decimalsMap.has(m) && !_pendingDecimalsFetch.has(m));
  if (!unknown.length) return;

  unknown.forEach(m => _pendingDecimalsFetch.add(m));
  try {
    const { rpcCall } = require('./lib/rpc-client');
    const resp = await rpcCall('getMultipleAccounts', [unknown, { encoding: 'base64', dataSlice: { offset: 44, length: 1 } }]);
    const accounts = resp?.value ?? [];
    accounts.forEach((acc, i) => {
      if (!acc?.data?.[0]) return;
      try {
        const buf = Buffer.from(acc.data[0], 'base64');
        if (buf.length >= 1) {
          const decimals = buf.readUInt8(0);
          if (decimals <= 18) {
            decimalsMap.set(unknown[i], decimals);
            // Update any vaultRegistry entries that were waiting on this mint
            for (const [vault, entry] of vaultRegistry) {
              if (entry.mint0 === unknown[i] && entry.dec0 === null) entry.dec0 = decimals;
              if (entry.mint1 === unknown[i] && entry.dec1 === null) entry.dec1 = decimals;
            }
          }
        }
      } catch {}
    });
  } catch { /* non-fatal — will retry on next pool update */ }
  finally {
    unknown.forEach(m => _pendingDecimalsFetch.delete(m));
  }
}

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

  // ── Try Raydium AMM v4 / CLMM ─────────────────────────────────────────────
  const poolR = raydium.processAccountEvent(event);
  if (poolR) {
    stats.pools++;
    const baseMint  = poolR.coinMint || poolR.mint0;
    const quoteMint = poolR.pcMint   || poolR.mint1;
    await redis.writePool(event.pubkey, poolR);

    if (poolR.price !== null && poolR.price !== undefined && poolR.price > 0 && isFinite(poolR.price)) {
      if (_isUsdQuote(quoteMint)) {
        // USD-quoted pool: write human USD price
        await redis.writePrice(baseMint, poolR.price, poolR.type);
        ohlcv.tick(baseMint, poolR.price, 0, event.ts);

      } else if (quoteMint === SOL_MINT) {
        // AMM v4: price = pcAmount/coinAmount adjusted = SOL per coinToken.
        // coinReserve/pcReserve stored as strings (BigInt) in parser output.
        await redis.writePriceSol(baseMint, poolR.price, poolR.type, {
          poolAccount:  event.pubkey,
          coinReserve:  poolR.coinReserve,
          pcReserve:    poolR.pcReserve,
          coinDecimals: poolR.coinDecimals,
          pcDecimals:   poolR.pcDecimals,
        });
        await redis.writePoolByMint(baseMint, event.pubkey);

      } else if (baseMint === SOL_MINT && quoteMint && poolR.price > 0) {
        // Unusual: SOL is the "coin" side. price = pcAmount/coinAmount = TOKEN per SOL → invert.
        const priceSol = 1 / poolR.price;
        if (isFinite(priceSol) && priceSol > 0) {
          // Swap labels: coinMint=SOL so coinReserve is the SOL vault, pcReserve is the token vault.
          // Downstream convention: coinReserve=token side, pcReserve=SOL side — swap here.
          await redis.writePriceSol(quoteMint, priceSol, poolR.type, {
            poolAccount:  event.pubkey,
            coinReserve:  poolR.pcReserve,    // token side (pcMint = quoteMint = token)
            pcReserve:    poolR.coinReserve,  // SOL side (coinMint = SOL)
            coinDecimals: poolR.pcDecimals,
            pcDecimals:   poolR.coinDecimals, // 9
          });
          await redis.writePoolByMint(quoteMint, event.pubkey);
        }
      }
    }

    // CLMM: mint0/mint1 orientation — price = token1 per token0 (decimal-adjusted).
    if (poolR.type === 'raydium-clmm' && poolR.price > 0 && isFinite(poolR.price)) {
      if (poolR.mint1 === SOL_MINT) {
        // price = SOL per mint0 — direct
        await redis.writePriceSol(poolR.mint0, poolR.price, 'raydium-clmm', { poolAccount: event.pubkey });
        await redis.writePoolByMint(poolR.mint0, event.pubkey);
      } else if (poolR.mint0 === SOL_MINT) {
        // price = TOKEN per SOL → invert for SOL per TOKEN
        const priceSol = 1 / poolR.price;
        if (isFinite(priceSol) && priceSol > 0) {
          await redis.writePriceSol(poolR.mint1, priceSol, 'raydium-clmm', { poolAccount: event.pubkey });
          await redis.writePoolByMint(poolR.mint1, event.pubkey);
        }
      }
    }

    await postgres.writePool({ ...poolR, program: event.owner, slot: event.slot });
    return;
  }

  // ── Try Orca Whirlpool ────────────────────────────────────────────────────
  const poolO = orca.processAccountEvent(event);
  if (poolO) {
    stats.pools++;
    if (poolO.mintA && decimalsMap.has(poolO.mintA)) poolO.decimalsA = decimalsMap.get(poolO.mintA);
    if (poolO.mintB && decimalsMap.has(poolO.mintB)) poolO.decimalsB = decimalsMap.get(poolO.mintB);

    // If decimals still unknown, fetch from on-chain and cache for future updates
    if (poolO.decimalsA === null || poolO.decimalsB === null) {
      await _fetchAndCacheDecimals([poolO.mintA, poolO.mintB]);
      if (poolO.mintA && decimalsMap.has(poolO.mintA)) poolO.decimalsA = decimalsMap.get(poolO.mintA);
      if (poolO.mintB && decimalsMap.has(poolO.mintB)) poolO.decimalsB = decimalsMap.get(poolO.mintB);
    }

    if (poolO.decimalsA !== null && poolO.decimalsB !== null && poolO.sqrtPriceX64) {
      poolO.price = orca.whirlpoolPrice(BigInt(poolO.sqrtPriceX64), poolO.decimalsA, poolO.decimalsB);
    }
    await redis.writePool(event.pubkey, poolO);

    if (poolO.price !== null && poolO.price !== undefined && poolO.price > 0 && isFinite(poolO.price)) {
      if (_isUsdQuote(poolO.mintB)) {
        // mintB is USDC/USDT: price = USD per mintA — write USD price
        await redis.writePrice(poolO.mintA, poolO.price, 'orca-whirlpool');
        ohlcv.tick(poolO.mintA, poolO.price, 0, event.ts);

      } else if (poolO.mintB === SOL_MINT) {
        // whirlpoolPrice = mintB per mintA (decimal-adjusted). mintB=SOL → price = SOL per mintA.
        await redis.writePriceSol(poolO.mintA, poolO.price, 'orca-whirlpool', { poolAccount: event.pubkey });
        await redis.writePoolByMint(poolO.mintA, event.pubkey);

      } else if (poolO.mintA === SOL_MINT) {
        // mintA=SOL, mintB=TOKEN: price = TOKEN per SOL → invert
        const priceSol = 1 / poolO.price;
        if (isFinite(priceSol) && priceSol > 0) {
          await redis.writePriceSol(poolO.mintB, priceSol, 'orca-whirlpool', { poolAccount: event.pubkey });
          await redis.writePoolByMint(poolO.mintB, event.pubkey);
        }
      }
    }

    await postgres.writePool({ ...poolO, program: event.owner, slot: event.slot });
    return;
  }

  // ── Try Raydium CPMM ─────────────────────────────────────────────────────
  const poolC = cpmm.processAccountEvent(event);
  if (poolC) {
    stats.pools++;
    // Fetch unknown decimals before registering vaults — needed for price computation
    if (!decimalsMap.has(poolC.mint0) || !decimalsMap.has(poolC.mint1)) {
      await _fetchAndCacheDecimals([poolC.mint0, poolC.mint1]);
    }
    // Register vaults so we can compute price when vault balances arrive
    const dec0 = poolC.dec0 ?? decimalsMap.get(poolC.mint0) ?? null;
    const dec1 = poolC.dec1 ?? decimalsMap.get(poolC.mint1) ?? null;
    const entry = { poolAccount: event.pubkey, mint0: poolC.mint0, mint1: poolC.mint1, dec0, dec1, poolType: 'raydium-cpmm' };
    vaultRegistry.set(poolC.vault0, { ...entry, isVault0: true,  pairedVault: poolC.vault1 });
    vaultRegistry.set(poolC.vault1, { ...entry, isVault0: false, pairedVault: poolC.vault0 });
    await redis.writePool(event.pubkey, poolC);
    return;
  }

  // ── Try PumpSwap AMM ─────────────────────────────────────────────────────
  const poolPS = pumpswap.processAccountEvent(event);
  if (poolPS) {
    stats.pools++;
    // base_mint decimals not in pool — fetch via RPC if not already cached
    if (!decimalsMap.has(poolPS.baseMint)) {
      await _fetchAndCacheDecimals([poolPS.baseMint]);
    }
    const decBase  = decimalsMap.get(poolPS.baseMint) ?? null;
    const decQuote = decimalsMap.get(poolPS.quoteMint) ?? 9; // WSOL = 9
    const entry = {
      poolAccount: event.pubkey,
      mint0: poolPS.baseMint,
      mint1: poolPS.quoteMint,
      dec0:  decBase,
      dec1:  decQuote,
      poolType: 'pumpswap',
    };
    vaultRegistry.set(poolPS.baseVault,  { ...entry, isVault0: true,  pairedVault: poolPS.quoteVault });
    vaultRegistry.set(poolPS.quoteVault, { ...entry, isVault0: false, pairedVault: poolPS.baseVault  });
    await redis.writePool(event.pubkey, { ...poolPS, poolType: 'pumpswap' });
    // baseMint = token, quoteMint = WSOL (or USDC on USD-quoted pools). Index the TOKEN → pool
    // mapping, and only when the quote is SOL so this pool can yield a SOL price. NEVER index the
    // quote currency (WSOL/USDC) — doing so hijacks the reverse index for major tokens and poisons
    // their price resolution. (The vault handler re-affirms baseMint → pool once balances arrive.)
    if (poolPS.quoteMint === SOL_MINT) {
      await redis.writePoolByMint(poolPS.baseMint, event.pubkey);
    }
    return;
  }

  // ── Try CPMM vault token account (Token/Token-2022 program) ───────────────
  // When a vault balance changes after a swap, recompute pool price.
  if (event.owner === 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' ||
      event.owner === 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb') {
    const reg = vaultRegistry.get(event.pubkey);
    if (reg && reg.dec0 !== null && reg.dec1 !== null) {
      try {
        const vaultBuf = Buffer.from(bs58.decode(event.data));
        if (vaultBuf.length >= 72) {
          const balance = Number(vaultBuf.readBigUInt64LE(64)); // SPL token balance at offset 64
          // Fetch the other vault's last known balance from Redis
          const poolData = await redis.getPool(reg.poolAccount);
          if (poolData) {
            let amt0, amt1;
            if (reg.isVault0) {
              amt0 = balance;
              amt1 = poolData._vault1Balance ?? null;
            } else {
              amt0 = poolData._vault0Balance ?? null;
              amt1 = balance;
            }
            // Store updated balance in pool record
            const updated = {
              ...poolData,
              _vault0Balance: reg.isVault0 ? balance : (poolData._vault0Balance ?? null),
              _vault1Balance: reg.isVault0 ? (poolData._vault1Balance ?? null) : balance,
            };
            if (amt0 !== null && amt1 !== null && amt0 > 0) {
              const human0 = amt0 / Math.pow(10, reg.dec0);
              const human1 = amt1 / Math.pow(10, reg.dec1);
              updated.price = human1 / human0;
              if (updated.price > 0 && isFinite(updated.price)) {
                // updated.price = human1/human0 = mint1 per mint0 (decimal-adjusted)
                const src = reg.poolType || 'raydium-cpmm';
                if (_isUsdQuote(reg.mint1)) {
                  await redis.writePrice(reg.mint0, updated.price, src);
                  ohlcv.tick(reg.mint0, updated.price, 0, event.ts);
                } else if (reg.mint1 === SOL_MINT) {
                  // mint1=SOL: price = SOL per mint0 — direct
                  await redis.writePriceSol(reg.mint0, updated.price, src, {
                    poolAccount:  reg.poolAccount,
                    coinReserve:  amt0,
                    pcReserve:    amt1,
                    coinDecimals: reg.dec0,
                    pcDecimals:   reg.dec1,
                  });
                  await redis.writePoolByMint(reg.mint0, reg.poolAccount);
                } else if (reg.mint0 === SOL_MINT) {
                  // mint0=SOL: price = mint1 per SOL → invert
                  const priceSol = 1 / updated.price;
                  if (isFinite(priceSol) && priceSol > 0) {
                    await redis.writePriceSol(reg.mint1, priceSol, src, {
                      poolAccount:  reg.poolAccount,
                      coinReserve:  amt1,
                      pcReserve:    amt0,
                      coinDecimals: reg.dec1,
                      pcDecimals:   reg.dec0,
                    });
                    await redis.writePoolByMint(reg.mint1, reg.poolAccount);
                  }
                }
                await redis.updateTrending(reg.mint0, human1); // volume proxy
                ohlcv.tick(reg.mint0, updated.price, 0, event.ts);
              }
            }
            await redis.writePool(reg.poolAccount, updated);
          }
        }
      } catch { /* non-fatal */ }
      return;
    }
  }

  // ── Try Pump.fun bonding curve ────────────────────────────────────────────
  // Writes circuit:pool:{bcAddress} with virtualSolReserves/virtualTokenReserves.
  // Mint→pool mapping (circuit:pool-by-mint) is NOT written here because the
  // bonding curve account does not contain the mint address. Instead, the
  // circuit-price-feed /register endpoint writes this mapping when a circuit-agent
  // scans a Pump.fun token and provides its pair address.
  const poolP = pumpfun.processAccountEvent(event);
  if (poolP) {
    stats.pools++;
    await redis.writePool(event.pubkey, poolP);
    return;
  }

  // ── Try token mint ────────────────────────────────────────────────────────
  const mintInfo = token.processAccountEvent(event);
  if (mintInfo) {
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
  if (!event.success) return;

  const { preTokenBalances, postTokenBalances, accounts } = event;
  if (!accounts?.length || !preTokenBalances?.length || !postTokenBalances?.length) return;

  // Build pubkey → amount maps from accountIndex lookups
  const preMap  = new Map();
  const postMap = new Map();
  for (const b of preTokenBalances) {
    const pk = accounts[b.accountIndex];
    if (pk) preMap.set(pk, BigInt(b.amount || '0'));
  }
  for (const b of postTokenBalances) {
    const pk = accounts[b.accountIndex];
    if (pk) postMap.set(pk, BigInt(b.amount || '0'));
  }

  // Check each account in this transaction against vaultRegistry.
  // Only process base vaults (isVault0 = true) to avoid double-counting per swap.
  for (const pk of postMap.keys()) {
    const reg = vaultRegistry.get(pk);
    if (!reg || !reg.isVault0) continue;

    const preAmt  = preMap.get(pk) ?? 0n;
    const postAmt = postMap.get(pk);
    if (preAmt === postAmt) continue;

    // Base vault decreased → tokens flowed to buyer (BUY); increased → SELL
    const isBuy = postAmt < preAmt;

    // Compute SOL volume from paired quote vault (WSOL) delta
    let volumeSol = 0;
    if (reg.pairedVault) {
      const quotePre  = preMap.get(reg.pairedVault)  ?? 0n;
      const quotePost = postMap.get(reg.pairedVault);
      if (quotePost !== undefined && quotePost !== quotePre) {
        const quoteDelta = quotePost > quotePre ? quotePost - quotePre : quotePre - quotePost;
        volumeSol = Number(quoteDelta) / 1e9;
      }
    }

    // Get current price for the OHLCV tick
    const poolData = await redis.getPool(reg.poolAccount);
    const price    = poolData?.price;
    if (!price || price <= 0 || !isFinite(price)) continue;

    ohlcv.tick(reg.mint0, price, volumeSol, event.ts, isBuy);
    if (volumeSol > 0) redis.updateTrending(reg.mint0, volumeSol).catch(() => {});
  }
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
