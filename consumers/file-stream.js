// consumers/file-stream.js — Read Geyser events from a JSON-lines file.
//
// Used when circuit-geyser is configured with backend=file or backend=stdout
// (redirect stdout to a file). Tails the file for new lines.
//
// Also works with stdout pipe:
//   solana-test-validator --geyser-plugin-config local.json 2>/dev/null | \
//     node indexer.js --consumer=stdin
'use strict';

const fs       = require('fs');
const readline = require('readline');
const Logger   = require('../lib/logger');

class FileStreamConsumer {
  /**
   * @param {string}   filePath   - Path to JSON-lines file to tail
   * @param {function} onEvent    - Called with parsed event object
   * @param {object}   [opts]
   * @param {number}   [opts.pollMs=500] - How often to check for new lines
   */
  constructor(filePath, onEvent, opts = {}) {
    this._path    = filePath;
    this._onEvent = onEvent;
    this._pollMs  = opts.pollMs ?? 500;
    this._offset  = 0;
    this._timer   = null;
    this._stats   = { events: 0, errors: 0, lastEventTs: null };
  }

  start() {
    Logger.info('FileStreamConsumer: starting', { file: this._path });
    // Start from end of file (tail behavior)
    try {
      this._offset = fs.statSync(this._path).size;
    } catch {
      this._offset = 0;
    }
    this._poll();
  }

  stop() {
    if (this._timer) clearTimeout(this._timer);
    this._timer = null;
  }

  stats() { return { ...this._stats }; }

  _poll() {
    try {
      const stat = fs.statSync(this._path);
      if (stat.size > this._offset) {
        const buf = Buffer.allocUnsafe(stat.size - this._offset);
        const fd  = fs.openSync(this._path, 'r');
        fs.readSync(fd, buf, 0, buf.length, this._offset);
        fs.closeSync(fd);
        this._offset = stat.size;

        const lines = buf.toString('utf8').split('\n');
        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const event = JSON.parse(line);
            this._stats.events++;
            this._stats.lastEventTs = Date.now();
            this._onEvent(event);
          } catch (e) {
            this._stats.errors++;
          }
        }
      }
    } catch {}
    this._timer = setTimeout(() => this._poll(), this._pollMs);
  }
}

// ── stdin consumer ────────────────────────────────────────────────────────────

class StdinConsumer {
  constructor(onEvent) {
    this._onEvent = onEvent;
    this._stats   = { events: 0, errors: 0 };
  }

  start() {
    Logger.info('StdinConsumer: reading from stdin');
    const rl = readline.createInterface({ input: process.stdin });
    rl.on('line', (line) => {
      if (!line.trim()) return;
      try {
        this._stats.events++;
        this._onEvent(JSON.parse(line));
      } catch {
        this._stats.errors++;
      }
    });
    rl.on('close', () => Logger.info('StdinConsumer: stdin closed'));
  }

  stop() {}
  stats() { return { ...this._stats }; }
}

module.exports = { FileStreamConsumer, StdinConsumer };
