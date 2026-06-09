// ohlcv.js — Aggregate price tick events into OHLCV candles.
//
// Buckets ticks into 1m, 5m, 1h windows. When a bucket closes, emits a
// completed candle that writers can persist to TimescaleDB / Postgres.
//
// Usage:
//   const agg = new OHLCVAggregator(onCandle);
//   agg.tick(mint, price, volumeUsd, timestamp);
'use strict';

const WINDOWS = [
  { name: '1m',  ms: 60_000       },
  { name: '5m',  ms: 300_000      },
  { name: '1h',  ms: 3_600_000    },
  { name: '1d',  ms: 86_400_000   },
];

class OHLCVAggregator {
  /**
   * @param {function} onCandle - called with (candle) when a bucket closes
   *   candle: { mint, window, openTime, closeTime, open, high, low, close, volume, ticks }
   */
  constructor(onCandle) {
    this._onCandle = onCandle;
    // Map<window> → Map<mint> → bucket
    this._buckets = new Map();
    for (const w of WINDOWS) {
      this._buckets.set(w.name, new Map());
    }
  }

  /**
   * Record a price tick for a token.
   * @param {string} mint        - token mint address
   * @param {number} price       - price in USD
   * @param {number} volumeUsd   - trade volume in USD for this tick
   * @param {number} [ts]        - unix ms timestamp (default: now)
   */
  tick(mint, price, volumeUsd = 0, ts = Date.now()) {
    for (const w of WINDOWS) {
      const bucketTime = Math.floor(ts / w.ms) * w.ms;
      const map = this._buckets.get(w.name);

      const key = `${mint}:${bucketTime}`;
      let bucket = map.get(key);

      if (!bucket) {
        // Close any previous bucket for this mint+window
        this._closePrevious(map, mint, bucketTime, w);
        bucket = {
          mint,
          window:    w.name,
          openTime:  bucketTime,
          closeTime: bucketTime + w.ms,
          open:      price,
          high:      price,
          low:       price,
          close:     price,
          volume:    volumeUsd,
          ticks:     1,
        };
        map.set(key, bucket);
      } else {
        if (price > bucket.high)  bucket.high  = price;
        if (price < bucket.low)   bucket.low   = price;
        bucket.close   = price;
        bucket.volume += volumeUsd;
        bucket.ticks++;
      }
    }
  }

  _closePrevious(map, mint, currentBucketTime, w) {
    // Find and emit any open bucket for this mint in a different time slot
    for (const [key, bucket] of map.entries()) {
      if (bucket.mint === mint && bucket.openTime !== currentBucketTime) {
        map.delete(key);
        try { this._onCandle(bucket); } catch {}
      }
    }
  }

  /** Flush all open buckets (call on shutdown or periodically). */
  flush() {
    for (const [winName, map] of this._buckets.entries()) {
      for (const [key, bucket] of map.entries()) {
        map.delete(key);
        try { this._onCandle(bucket); } catch {}
      }
    }
  }

  /**
   * Evict buckets older than maxAgeMs. Prevents unbounded memory growth from
   * tokens that ticked once and never traded again.
   * Call periodically: setInterval(() => ohlcv.evictStale(), 10 * 60_000)
   */
  evictStale(maxAgeMs = 2 * 3_600_000) {
    const cutoff = Date.now() - maxAgeMs;
    let evicted = 0;
    for (const map of this._buckets.values()) {
      for (const [key, bucket] of map.entries()) {
        if (bucket.openTime < cutoff) {
          map.delete(key);
          try { this._onCandle(bucket); } catch {}
          evicted++;
        }
      }
    }
    return evicted;
  }

  /** Current total open bucket count across all windows. */
  bucketCount() {
    let n = 0;
    for (const map of this._buckets.values()) n += map.size;
    return n;
  }

  /** Returns current incomplete bucket for a mint+window (for live price reads). */
  getCurrent(mint, windowName = '1m') {
    const map = this._buckets.get(windowName);
    if (!map) return null;
    const ts = Date.now();
    const w  = WINDOWS.find(w => w.name === windowName);
    if (!w) return null;
    const bucketTime = Math.floor(ts / w.ms) * w.ms;
    return map.get(`${mint}:${bucketTime}`) ?? null;
  }
}

module.exports = { OHLCVAggregator, WINDOWS };
