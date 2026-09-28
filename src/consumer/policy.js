// Pure delivery-policy helpers (unit tested in test/policy.test.js).

/**
 * Maps a webhook call result onto the consumer's next move:
 *  SUCCESS      2xx                         → acknowledge
 *  RATE_LIMITED 429                         → cool down, retry without using the retry budget
 *  RETRYABLE    5xx, 408, timeout, network  → exponential backoff, then mark the client DOWN
 *  PERMANENT    any other 4xx               → dead-letter; retrying cannot help
 */
export function classifyResult({ statusCode, error, retryAfter }, now = Date.now()) {
  if (error) return { kind: 'RETRYABLE', reason: error };
  if (statusCode >= 200 && statusCode < 300) return { kind: 'SUCCESS', reason: `HTTP ${statusCode}` };
  if (statusCode === 429) {
    return { kind: 'RATE_LIMITED', reason: 'HTTP 429', retryAfterMs: parseRetryAfter(retryAfter, now) };
  }
  if (statusCode === 408 || statusCode >= 500) return { kind: 'RETRYABLE', reason: `HTTP ${statusCode}` };
  return { kind: 'PERMANENT', reason: `HTTP ${statusCode}` };
}

/** Retry-After is either delay-seconds or an HTTP date (RFC 9110, section 10.2.3). */
export function parseRetryAfter(value, now = Date.now()) {
  if (value === null || value === undefined || String(value).trim() === '') return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, Math.round(seconds * 1000));
  const date = Date.parse(value);
  return Number.isFinite(date) ? Math.max(0, date - now) : null;
}

/** base * 2^(attempt - 1), capped at maxBackoffMs, with ±10% jitter. */
export function backoffDelay(attempt, { baseBackoffMs, maxBackoffMs }, random = Math.random) {
  const exponential = Math.min(maxBackoffMs, baseBackoffMs * 2 ** Math.max(0, attempt - 1));
  return Math.round(exponential * (0.9 + random() * 0.2));
}

/**
 * Additive increase / multiplicative decrease (as in TCP congestion control):
 * halve the rate on every 429, creep back up by a fixed step on every success.
 */
export function nextAdaptiveRate(rate, outcome, { min = 0.5, max = 50, increase = 0.25, decrease = 0.5 } = {}) {
  const next = outcome === 'throttled' ? rate * decrease : rate + increase;
  return Math.round(Math.min(max, Math.max(min, next)) * 100) / 100;
}
