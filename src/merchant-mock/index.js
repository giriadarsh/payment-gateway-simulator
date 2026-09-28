// Mock merchant (client) endpoints. Each client exposes a webhook and a health
// endpoint whose behaviour can be switched at runtime to simulate outages,
// transient failures and rate limiting.
import { CLIENTS, PORTS } from '../shared/config.js';
import { createHttpApp, HANDLED, HttpError, sendJson } from '../shared/http.js';
import { createEventPublisher, createRedis } from '../shared/redis.js';
import { createLogger, onShutdown, sleep } from '../shared/util.js';

const MODES = ['HEALTHY', 'DOWN', 'FLAKY', 'RATE_LIMITED'];
const FAILURE_STYLES = {
  HTTP_503: '503 Service Unavailable',
  HTTP_500: '500 Internal Server Error',
  TIMEOUT: 'no response (request hangs)',
  CONNECTION_RESET: 'connection reset',
};
const DEFAULT_BEHAVIOR = {
  mode: 'HEALTHY',
  failureStyle: 'HTTP_503',
  failNext: 0,
  rateLimitPerSec: 2,
  retryAfterSec: 1,
  latencyMs: 60,
  recoverAt: null,
};
const SEEN_LIMIT = 5000;

const log = createLogger('merchant-mock');
const redis = createRedis('merchant-mock');
const emit = createEventPublisher(redis, 'merchant');

const freshClient = (client) => ({
  ...client,
  behavior: { ...DEFAULT_BEHAVIOR },
  bucket: null,
  seen: new Set(),
  stats: { received: 0, duplicates: 0, rejected: 0, rateLimited: 0, healthChecks: 0 },
});

const clients = new Map(CLIENTS.map((client) => [client.id, freshClient(client)]));

function getMockClient(id) {
  const client = clients.get(id);
  if (!client) throw new HttpError(404, `Unknown client "${id}"`);
  return client;
}

function describe(behavior) {
  switch (behavior.mode) {
    case 'DOWN': {
      const recovery = behavior.recoverAt ? `, recovers in ${Math.round((behavior.recoverAt - Date.now()) / 1000)} s` : '';
      return `down — ${FAILURE_STYLES[behavior.failureStyle]}${recovery}`;
    }
    case 'FLAKY':
      return `flaky — next ${behavior.failNext} request(s) fail with ${FAILURE_STYLES[behavior.failureStyle]}`;
    case 'RATE_LIMITED':
      return `rate limited to ${behavior.rateLimitPerSec} req/s (Retry-After: ${behavior.retryAfterSec}s)`;
    default:
      return 'healthy';
  }
}

function publicView(client) {
  const { recoverAt, ...behavior } = client.behavior;
  return {
    id: client.id,
    name: client.name,
    behavior: { ...behavior, recoverInMs: recoverAt ? Math.max(0, recoverAt - Date.now()) : null },
    description: describe(client.behavior),
    stats: client.stats,
  };
}

function setBehavior(client, behavior, reason) {
  client.behavior = behavior;
  client.bucket = null;
  emit('BEHAVIOR_CHANGED', {
    clientId: client.id,
    behavior: publicView(client).behavior,
    msg: `${client.name} webhook is now ${describe(behavior)}${reason ? ` (${reason})` : ''}`,
  });
}

function updateBehavior(client, patch = {}) {
  const next = { ...client.behavior };
  if (patch.mode !== undefined) {
    if (!MODES.includes(patch.mode)) throw new HttpError(400, `mode must be one of ${MODES.join(', ')}`);
    next.mode = patch.mode;
  }
  if (patch.failureStyle !== undefined) {
    if (!(patch.failureStyle in FAILURE_STYLES)) {
      throw new HttpError(400, `failureStyle must be one of ${Object.keys(FAILURE_STYLES).join(', ')}`);
    }
    next.failureStyle = patch.failureStyle;
  }
  for (const key of ['failNext', 'rateLimitPerSec', 'retryAfterSec', 'latencyMs']) {
    if (patch[key] === undefined) continue;
    const value = Number(patch[key]);
    if (!Number.isFinite(value) || value < 0) throw new HttpError(400, `${key} must be a non-negative number`);
    next[key] = value;
  }
  if (patch.recoverAfterMs !== undefined) {
    const ms = Number(patch.recoverAfterMs);
    next.recoverAt = ms > 0 ? Date.now() + ms : null;
  }
  if (next.mode === 'HEALTHY') next.recoverAt = null;

  // Only report a change when the behaviour actually differs.
  const changed = JSON.stringify(next) !== JSON.stringify(client.behavior);
  if (changed) setBehavior(client, next);
  return publicView(client);
}

/** Server-side token bucket the mock uses to decide when to answer 429. */
function takeToken(client) {
  const rate = Math.max(0.1, client.behavior.rateLimitPerSec);
  const capacity = Math.max(1, rate);
  const now = Date.now();
  const bucket = client.bucket ?? { tokens: capacity, ts: now };
  bucket.tokens = Math.min(capacity, bucket.tokens + ((now - bucket.ts) / 1000) * rate);
  bucket.ts = now;
  client.bucket = bucket;
  if (bucket.tokens < 1) return false;
  bucket.tokens -= 1;
  return true;
}

/** Fails a request in the configured style. Always returns HANDLED. */
function fail(req, res, style) {
  switch (style) {
    case 'TIMEOUT':
      // Never answer; drop the socket later so hung connections do not pile up.
      setTimeout(() => res.destroy(), 30_000).unref();
      break;
    case 'CONNECTION_RESET':
      req.socket.destroy();
      break;
    case 'HTTP_500':
      sendJson(res, 500, { error: 'Internal Server Error' });
      break;
    default:
      sendJson(res, 503, { error: 'Service Unavailable' });
  }
  return HANDLED;
}

function reject(client, req, res, info) {
  const style = client.behavior.failureStyle;
  client.stats.rejected += 1;
  emit('WEBHOOK_REJECTED', {
    clientId: client.id,
    txnId: info.txnId,
    attempt: info.attempt,
    style,
    level: 'debug',
    msg: `${client.name} failed ${info.txnId}: ${FAILURE_STYLES[style]}`,
  });
  return fail(req, res, style);
}

function remember(client, eventId) {
  const duplicate = client.seen.has(eventId);
  client.seen.add(eventId);
  if (client.seen.size > SEEN_LIMIT) client.seen.delete(client.seen.values().next().value);
  return duplicate;
}

const app = createHttpApp();

app.get('/healthz', () => ({ ok: true }));

app.post('/merchants/:id/webhook', async ({ req, res, params, body }) => {
  const client = getMockClient(params.id);
  const eventId = req.headers['idempotency-key'] ?? body?.id;
  const info = { txnId: body?.data?.transactionId, attempt: Number(req.headers['x-notification-attempt'] ?? 1) };

  // Rate limiting happens on arrival, like an API gateway in front of the client.
  if (client.behavior.mode === 'RATE_LIMITED' && !takeToken(client)) {
    client.stats.rateLimited += 1;
    emit('WEBHOOK_REJECTED', {
      clientId: client.id,
      ...info,
      statusCode: 429,
      level: 'debug',
      msg: `${client.name} answered 429 Too Many Requests for ${info.txnId} (limit ${client.behavior.rateLimitPerSec} req/s)`,
    });
    sendJson(res, 429, { error: 'Too Many Requests' }, { 'retry-after': String(client.behavior.retryAfterSec) });
    return HANDLED;
  }

  await sleep(client.behavior.latencyMs * (0.7 + Math.random() * 0.6));
  const behavior = client.behavior;

  if (behavior.mode === 'DOWN') return reject(client, req, res, info);
  if (behavior.mode === 'FLAKY') {
    if (behavior.failNext > 0) {
      behavior.failNext -= 1;
      return reject(client, req, res, info);
    }
    setBehavior(client, { ...behavior, mode: 'HEALTHY' }, 'transient errors are over');
  }

  const duplicate = eventId ? remember(client, eventId) : false;
  client.stats.received += 1;
  if (duplicate) client.stats.duplicates += 1;
  emit('WEBHOOK_RECEIVED', {
    clientId: client.id,
    ...info,
    duplicate,
    level: 'debug',
    msg: `${client.name} received ${info.txnId}${duplicate ? ' again (duplicate, same Idempotency-Key)' : ''} → 200 OK`,
  });
  return { received: true, duplicate };
});

app.get('/merchants/:id/health', async ({ req, res, params }) => {
  const client = getMockClient(params.id);
  client.stats.healthChecks += 1;
  await sleep(10 + Math.random() * 20);
  if (client.behavior.mode === 'DOWN') return fail(req, res, client.behavior.failureStyle);
  return { status: 'ok' };
});

app.get('/control', () => ({ clients: [...clients.values()].map(publicView) }));

app.put('/control/:id', ({ params, body }) => updateBehavior(getMockClient(params.id), body));

app.post('/control/reset', () => {
  for (const [id, client] of clients) clients.set(id, freshClient(client));
  emit('MERCHANTS_RESET', { level: 'debug', msg: 'All mock merchants reset to healthy' });
  return { ok: true };
});

// Scheduled recoveries ("the client comes back after N seconds").
setInterval(() => {
  const now = Date.now();
  for (const client of clients.values()) {
    const { recoverAt } = client.behavior;
    if (recoverAt && recoverAt <= now) {
      setBehavior(client, { ...client.behavior, mode: 'HEALTHY', failNext: 0, recoverAt: null }, 'outage over');
    }
  }
}, 200).unref();

onShutdown(log, async () => {
  app.server.close();
  redis.disconnect();
});

await app.listen(PORTS.merchant);
log.info(`mock merchants listening on :${PORTS.merchant} (${CLIENTS.map((c) => c.id).join(', ')})`);
