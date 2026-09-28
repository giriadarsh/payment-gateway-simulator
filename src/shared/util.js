import { randomBytes } from 'node:crypto';

/** Sleep that can be cut short by an AbortSignal (resolves, never rejects). */
export function sleep(ms, signal) {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const timer = setTimeout(done, Math.max(0, ms));
    function done() {
      clearTimeout(timer);
      signal?.removeEventListener('abort', done);
      resolve();
    }
    signal?.addEventListener('abort', done, { once: true });
  });
}

/** Short, sortable, human-friendly id: prefix + base36 time + random suffix. */
export function newId(prefix) {
  return `${prefix}_${Date.now().toString(36)}${randomBytes(3).toString('hex')}`;
}

export const pick = (items) => items[Math.floor(Math.random() * items.length)];

export const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

export const formatMs = (ms) => (ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(1)} s`);

export function createLogger(service) {
  const line = (level, message, extra) =>
    `${new Date().toISOString()} ${level.padEnd(5)} [${service}] ${message}${
      extra === undefined ? '' : ` ${JSON.stringify(extra)}`
    }`;
  return {
    info: (message, extra) => console.log(line('INFO', message, extra)),
    warn: (message, extra) => console.warn(line('WARN', message, extra)),
    error: (message, extra) => console.error(line('ERROR', message, extra)),
  };
}

/** Run `fn` on SIGTERM/SIGINT, then exit. */
export function onShutdown(log, fn) {
  let stopping = false;
  for (const signal of ['SIGTERM', 'SIGINT']) {
    process.on(signal, async () => {
      if (stopping) return;
      stopping = true;
      log.info(`${signal} received, shutting down`);
      try {
        await fn();
      } catch (err) {
        log.error('shutdown failed', { error: err.message });
      }
      process.exit(0);
    });
  }
}
