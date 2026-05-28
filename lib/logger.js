'use strict';

const LEVEL = process.env.LOG_LEVEL || 'info';
const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
const cur = LEVELS[LEVEL] ?? 1;

function log(level, msg, meta = {}) {
  if ((LEVELS[level] ?? 1) < cur) return;
  const line = { ts: new Date().toISOString(), level, msg, ...meta };
  const out = level === 'error' ? process.stderr : process.stdout;
  out.write(JSON.stringify(line) + '\n');
}

module.exports = {
  debug: (msg, m) => log('debug', msg, m),
  info:  (msg, m) => log('info',  msg, m),
  warn:  (msg, m) => log('warn',  msg, m),
  error: (msg, m) => log('error', msg, m),
};
