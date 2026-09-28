import { getClient } from '../shared/config.js';
import { K } from '../shared/keys.js';
import { nextAdaptiveRate } from './policy.js';

// Token bucket kept in Redis so every consumer instance shares one budget per
// client. Uses Redis TIME, so instances with skewed clocks still agree.
// Returns { allowed (0|1), waitMs, tokens }.
const TAKE_TOKEN = `
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1]) or capacity
local ts = tonumber(state[2]) or now
tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate / 1000)
local allowed = 0
local wait = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  wait = math.ceil((1 - tokens) * 1000 / rate)
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], 60000)
return { allowed, wait, tostring(tokens) }
`;

/** Pace slightly below the contracted limit so network jitter does not trigger 429s. */
export const CONTRACT_SAFETY_MARGIN = 0.9;

export function createRateLimiter(redis) {
  redis.defineCommand('takeToken', { numberOfKeys: 1, lua: TAKE_TOKEN });

  /** Requests/sec to pace at: contracted (token bucket) or discovered (adaptive). */
  async function rateFor(clientId, settings) {
    if (settings.rateLimitStrategy === 'adaptive') {
      const discovered = Number(await redis.get(K.clientAdaptiveRate(clientId)));
      return discovered > 0 ? discovered : settings.adaptiveStartRate;
    }
    const stored = Number(await redis.get(K.clientRateLimit(clientId)));
    const contracted = stored > 0 ? stored : (getClient(clientId)?.rateLimitPerSec ?? 10);
    return Math.round(contracted * CONTRACT_SAFETY_MARGIN * 100) / 100;
  }

  async function take(clientId, rate) {
    // Capacity of one second's worth of tokens allows a small burst.
    const [allowed, waitMs, tokens] = await redis.takeToken(K.clientBucket(clientId), rate, Math.max(1, rate));
    return { allowed: allowed === 1, waitMs: Number(waitMs), tokens: Number(tokens) };
  }

  /** Adaptive strategy only: adjust the client's rate after a success or a 429. */
  async function adapt(clientId, outcome, settings) {
    if (settings.rateLimitStrategy !== 'adaptive') return null;
    const from = await rateFor(clientId, settings);
    const to = nextAdaptiveRate(from, outcome);
    if (to !== from) await redis.set(K.clientAdaptiveRate(clientId), String(to), 'EX', 3600);
    return { from, to };
  }

  return { rateFor, take, adapt };
}
