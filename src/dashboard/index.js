// Dashboard: serves the UI, exposes the control API and streams events and
// state snapshots to the browser over Server-Sent Events.
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  CLIENTS,
  CONSUMER_GROUP,
  DEFAULT_SETTINGS,
  getClient,
  MERCHANT_URL,
  PARTITIONS,
  PORTS,
  PRODUCER_URL,
  RATE_LIMIT_STRATEGIES,
  TOPIC,
} from '../shared/config.js';
import { createHttpApp, HANDLED, HttpError, requestJson } from '../shared/http.js';
import { createKafka, ensureTopic } from '../shared/kafka.js';
import { CH, K } from '../shared/keys.js';
import { createEventPublisher, createRedis, parseSettings } from '../shared/redis.js';
import { createLogger, onShutdown, sleep } from '../shared/util.js';
import { createScenarioRunner, scenarioCatalog } from './scenarios.js';
import { createStateCollector } from './state.js';

const SNAPSHOT_INTERVAL_MS = 600;
const HISTORY_LIMIT = 1500;
const PUBLIC_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../public');
const STATIC_FILES = {
  '/': ['index.html', 'text/html; charset=utf-8'],
  '/index.html': ['index.html', 'text/html; charset=utf-8'],
  '/styles.css': ['styles.css', 'text/css; charset=utf-8'],
  '/app.js': ['app.js', 'text/javascript; charset=utf-8'],
};

/** Allowed ranges for settings edited from the UI. */
const SETTING_LIMITS = {
  maxAttempts: [1, 10],
  baseBackoffMs: [100, 10_000],
  maxBackoffMs: [500, 60_000],
  requestTimeoutMs: [200, 10_000],
  defaultRetryAfterMs: [100, 30_000],
  adaptiveStartRate: [0.5, 50],
  healthCheckIntervalMs: [500, 30_000],
  healthyThreshold: [1, 5],
};

const log = createLogger('dashboard');
const redis = createRedis('dashboard');
const sub = createRedis('dashboard-sub');
const emit = createEventPublisher(redis, 'dashboard');
const kafka = createKafka('dashboard');
const collector = createStateCollector({ redis, kafka, log });

const streams = new Set();
const history = [];
let snapshot = null;

function broadcast(type, data) {
  const frame = `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of streams) res.write(frame);
}

async function setClientStatus(clientId, status, by = 'operator') {
  if (!getClient(clientId)) throw new HttpError(404, `Unknown client "${clientId}"`);
  if (!['UP', 'DOWN'].includes(status)) throw new HttpError(400, 'status must be UP or DOWN');
  await redis.set(K.clientStatus(clientId), status);
  await redis.publish(CH.clientStatus, JSON.stringify({ clientId, status, by, ts: Date.now() }));
  emit('CLIENT_STATUS_SET', { clientId, status, by, msg: `SET ${K.clientStatus(clientId)} ${status} (by ${by})` });
}

async function updateSettings(patch = {}) {
  const values = {};
  for (const [key, value] of Object.entries(patch)) {
    if (key === 'rateLimitStrategy') {
      if (!RATE_LIMIT_STRATEGIES.includes(value)) throw new HttpError(400, `rateLimitStrategy must be one of ${RATE_LIMIT_STRATEGIES.join(', ')}`);
      values[key] = value;
    } else if (key in SETTING_LIMITS) {
      const number = Number(value);
      const [min, max] = SETTING_LIMITS[key];
      if (!Number.isFinite(number) || number < min || number > max) throw new HttpError(400, `${key} must be between ${min} and ${max}`);
      values[key] = String(number);
    } else {
      throw new HttpError(400, `Unknown setting "${key}"`);
    }
  }
  if (Object.keys(values).length) {
    await redis.hset(K.settings, values);
    emit('SETTINGS_UPDATED', { settings: values, level: 'debug', msg: `HSET ${K.settings} ${Object.entries(values).map(([k, v]) => `${k}=${v}`).join(' ')}` });
  }
  return parseSettings(await redis.hgetall(K.settings));
}

const scenarios = createScenarioRunner({ redis, emit, log, setClientStatus, updateSettings });

async function scanKeys(pattern) {
  const keys = [];
  let cursor = '0';
  do {
    const [next, batch] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 500);
    cursor = next;
    keys.push(...batch);
  } while (cursor !== '0');
  return keys;
}

/** Clears simulation state. Old Kafka messages are skipped thanks to the new epoch. */
async function resetSimulation() {
  scenarios.cancel('simulation reset');
  await requestJson(`${MERCHANT_URL}/control/reset`, { method: 'POST' }).catch((err) =>
    log.warn('could not reset merchants', { error: err.message }),
  );
  const keys = [...(await scanKeys('txn:*')), ...(await scanKeys('stats:client:*')), ...(await scanKeys('client:*'))];
  const multi = redis.multi().set(K.epoch, String(Date.now()));
  if (keys.length) multi.del(...keys);
  multi.del(K.recentTxns, K.holds, K.monitors).xtrim(K.healthCheckStream, 'MAXLEN', 0);
  for (const client of CLIENTS) multi.set(K.clientStatus(client.id), 'UP');
  await multi.exec();
  await redis.publish(CH.control, JSON.stringify({ type: 'RESET', ts: Date.now() }));
  history.length = 0;
  broadcast('reset', { ts: Date.now() });
  emit('SIMULATION_RESET', { msg: 'Simulation reset: every client is UP and healthy; any old backlog is skipped' });
}

async function serveStatic(req, res, url) {
  const entry = req.method === 'GET' ? STATIC_FILES[url.pathname] : undefined;
  if (!entry) return undefined;
  const [file, type] = entry;
  const body = await readFile(path.join(PUBLIC_DIR, file));
  res.writeHead(200, { 'content-type': type, 'cache-control': 'no-cache' });
  res.end(body);
  return HANDLED;
}

const app = createHttpApp({ fallback: serveStatic });

app.get('/healthz', () => ({ ok: true }));

app.get('/api/meta', () => ({
  topic: TOPIC,
  consumerGroup: CONSUMER_GROUP,
  partitions: PARTITIONS,
  clients: CLIENTS,
  scenarios: scenarioCatalog(),
  defaultSettings: DEFAULT_SETTINGS,
  strategies: RATE_LIMIT_STRATEGIES,
}));

app.get('/api/state', async () => snapshot ?? { ...(await collector.collect()), scenario: scenarios.current() });

app.get('/api/stream', ({ req, res }) => {
  res.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
  });
  res.write('retry: 2000\n\n');
  res.write(`event: hello\ndata: ${JSON.stringify({ events: history.slice(-400), snapshot })}\n\n`);
  streams.add(res);
  req.on('close', () => streams.delete(res));
  return HANDLED;
});

app.post('/api/scenarios/:id', ({ params, body }) => scenarios.start(params.id, body));
app.post('/api/scenario/cancel', () => {
  scenarios.cancel();
  return { ok: true };
});

app.post('/api/reset', async () => {
  await resetSimulation();
  return { ok: true };
});

app.post('/api/transactions', ({ body }) =>
  requestJson(`${PRODUCER_URL}/transactions`, {
    method: 'POST',
    body,
    timeoutMs: Number(body?.count ?? 1) * Number(body?.intervalMs ?? 0) + 30_000,
  }),
);

app.put('/api/clients/:id/behavior', ({ params, body }) => {
  if (!getClient(params.id)) throw new HttpError(404, `Unknown client "${params.id}"`);
  return requestJson(`${MERCHANT_URL}/control/${params.id}`, { method: 'PUT', body });
});

app.put('/api/clients/:id/status', async ({ params, body }) => {
  await setClientStatus(params.id, body?.status, 'operator');
  return { ok: true };
});

app.put('/api/settings', ({ body }) => updateSettings(body));
app.post('/api/settings/reset', async () => {
  await redis.del(K.settings);
  emit('SETTINGS_UPDATED', { level: 'debug', msg: `DEL ${K.settings} — defaults restored` });
  return parseSettings({});
});

async function snapshotLoop() {
  for (;;) {
    try {
      snapshot = { ...(await collector.collect()), scenario: scenarios.current() };
      broadcast('snapshot', snapshot);
    } catch (err) {
      log.warn('snapshot failed', { error: err.message });
    }
    await sleep(SNAPSHOT_INTERVAL_MS);
  }
}

async function main() {
  // "By default all clients are UP": create missing status keys and an initial epoch.
  const bootstrap = redis.multi().set(K.epoch, String(Date.now()), 'NX');
  for (const client of CLIENTS) bootstrap.set(K.clientStatus(client.id), 'UP', 'NX');
  await bootstrap.exec();

  await sub.subscribe(CH.events);
  sub.on('message', (_channel, raw) => {
    const event = JSON.parse(raw);
    history.push(event);
    if (history.length > HISTORY_LIMIT) history.splice(0, history.length - HISTORY_LIMIT);
    broadcast('sim', event);
  });

  setInterval(() => {
    for (const res of streams) res.write(': keep-alive\n\n');
  }, 15_000).unref();

  await app.listen(PORTS.dashboard);
  log.info(`dashboard on http://localhost:${PORTS.dashboard}`);
  await ensureTopic(kafka, log).catch((err) => log.warn('could not ensure topic yet', { error: err.message }));
  snapshotLoop();
}

onShutdown(log, async () => {
  for (const res of streams) res.end();
  await collector.disconnect();
  redis.disconnect();
  sub.disconnect();
});

main().catch((err) => {
  log.error('failed to start', { error: err.message });
  process.exit(1);
});
