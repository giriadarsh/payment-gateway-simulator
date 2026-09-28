import Redis from 'ioredis';
import { DEFAULT_SETTINGS, REDIS_URL } from './config.js';
import { CH, K } from './keys.js';

export function createRedis(name, options = {}) {
  const redis = new Redis(REDIS_URL, {
    connectionName: name,
    retryStrategy: (times) => Math.min(times * 200, 2000),
    ...options,
  });
  redis.on('error', (err) => console.error(`[redis:${name}] ${err.message}`));
  return redis;
}

let sequence = 0;

/**
 * Returns emit(type, data): publishes a visualisation event on the shared
 * `sim:events` channel. Events are fire-and-forget; they never affect delivery.
 */
export function createEventPublisher(redis, service) {
  const prefix = `${service}-${Date.now().toString(36)}`;
  return function emit(type, data = {}) {
    const event = { id: `${prefix}-${++sequence}`, ts: Date.now(), service, type, ...data };
    redis.publish(CH.events, JSON.stringify(event)).catch(() => {});
    return event;
  };
}

export function parseSettings(raw = {}) {
  const settings = { ...DEFAULT_SETTINGS };
  for (const [key, value] of Object.entries(raw)) {
    if (!(key in DEFAULT_SETTINGS)) continue;
    if (typeof DEFAULT_SETTINGS[key] === 'number') {
      const number = Number(value);
      if (Number.isFinite(number)) settings[key] = number;
    } else {
      settings[key] = value;
    }
  }
  return settings;
}

/** Settings reader with a short cache so hot paths do not hit Redis every call. */
export function createSettingsReader(redis, ttlMs = 500) {
  let cached = null;
  let loadedAt = 0;
  return async function getSettings() {
    if (cached && Date.now() - loadedAt < ttlMs) return cached;
    cached = parseSettings(await redis.hgetall(K.settings));
    loadedAt = Date.now();
    return cached;
  };
}
