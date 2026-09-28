// Health checker. Consumes "check client status" events from a Redis Stream
// (consumer group, so events survive restarts), probes the client's health
// endpoint until it is healthy again, then marks it UP and publishes the change
// so the consumer resumes that client's partition.
import { healthUrl, PORTS } from '../shared/config.js';
import { createHttpApp, describeFetchError } from '../shared/http.js';
import { CH, HEALTH_GROUP, K } from '../shared/keys.js';
import { createEventPublisher, createRedis, createSettingsReader } from '../shared/redis.js';
import { createLogger, onShutdown, sleep } from '../shared/util.js';

const CONSUMER_NAME = process.env.HEALTH_CHECKER_ID ?? 'checker-1';
const FLAG_TTL_MS = 30_000;
const CLAIM_IDLE_MS = 60_000;

const log = createLogger('health-checker');
const redis = createRedis('health-checker');
// Blocking XREADGROUP needs its own connection.
const reader = createRedis('health-checker-reader', { maxRetriesPerRequest: null });
const sub = createRedis('health-checker-sub');
const emit = createEventPublisher(redis, 'health-checker');
const getSettings = createSettingsReader(redis);

/** clientId -> active monitor */
const monitors = new Map();
let running = true;

async function ensureGroup() {
  try {
    await redis.xgroup('CREATE', K.healthCheckStream, HEALTH_GROUP, '$', 'MKSTREAM');
  } catch (err) {
    if (!String(err.message).includes('BUSYGROUP')) throw err;
  }
}

const toObject = (fields) => {
  const object = {};
  for (let i = 0; i < (fields?.length ?? 0); i += 2) object[fields[i]] = fields[i + 1];
  return object;
};

async function probe(clientId, timeoutMs) {
  const started = Date.now();
  try {
    const res = await fetch(healthUrl(clientId), { signal: AbortSignal.timeout(timeoutMs) });
    await res.arrayBuffer().catch(() => {});
    return { ok: res.ok, statusCode: res.status, label: `HTTP ${res.status}`, latencyMs: Date.now() - started };
  } catch (err) {
    return { ok: false, label: describeFetchError(err), latencyMs: Date.now() - started };
  }
}

function monitorView(monitor, settings) {
  return {
    clientId: monitor.clientId,
    startedAt: monitor.startedAt,
    probes: monitor.probes,
    consecutiveOk: monitor.consecutiveOk,
    required: settings.healthyThreshold,
    lastResult: monitor.lastResult,
    nextProbeAt: monitor.nextProbeAt,
    streamIds: monitor.entryIds,
  };
}

async function markUp(clientId) {
  await redis.multi().set(K.clientStatus(clientId), 'UP').del(K.clientHealthCheck(clientId)).exec();
  await redis.publish(CH.clientStatus, JSON.stringify({ clientId, status: 'UP', by: 'health-checker', ts: Date.now() }));
  emit('CLIENT_MARKED_UP', {
    clientId,
    msg: `${clientId} is healthy again → SET ${K.clientStatus(clientId)} UP and PUBLISH ${CH.clientStatus}`,
  });
}

async function finish(monitor, reason) {
  if (monitors.get(monitor.clientId) === monitor) monitors.delete(monitor.clientId);
  if (monitor.entryIds.length) await redis.xack(K.healthCheckStream, HEALTH_GROUP, ...monitor.entryIds);
  await redis.hdel(K.monitors, monitor.clientId);
  emit('HEALTH_MONITOR_STOPPED', {
    clientId: monitor.clientId,
    reason,
    probes: monitor.probes,
    level: 'debug',
    msg: `Stopped watching ${monitor.clientId} (${reason}) — XACK ${monitor.entryIds.length} stream event(s)`,
  });
}

async function runMonitor(monitor) {
  const { clientId, controller } = monitor;
  while (!controller.signal.aborted) {
    const settings = await getSettings();
    if ((await redis.get(K.clientStatus(clientId))) !== 'DOWN') return finish(monitor, 'client is already UP');

    // Keep the de-duplication flag alive while this monitor is running.
    await redis.set(K.clientHealthCheck(clientId), JSON.stringify({ by: CONSUMER_NAME, since: monitor.startedAt }), 'PX', FLAG_TTL_MS);

    const result = await probe(clientId, settings.requestTimeoutMs);
    if (controller.signal.aborted) break;
    monitor.probes += 1;
    monitor.consecutiveOk = result.ok ? monitor.consecutiveOk + 1 : 0;
    monitor.lastResult = result.label;
    monitor.nextProbeAt = Date.now() + settings.healthCheckIntervalMs;
    await redis.hset(K.monitors, clientId, JSON.stringify(monitorView(monitor, settings)));
    emit('HEALTH_PROBE', {
      clientId,
      ok: result.ok,
      result: result.label,
      probe: monitor.probes,
      consecutiveOk: monitor.consecutiveOk,
      required: settings.healthyThreshold,
      msg: `GET /health for ${clientId} → ${result.label} (${monitor.consecutiveOk}/${settings.healthyThreshold} healthy in a row)`,
    });

    if (monitor.consecutiveOk >= settings.healthyThreshold) {
      await markUp(clientId);
      return finish(monitor, 'recovered');
    }
    await sleep(settings.healthCheckIntervalMs, controller.signal);
  }
  return finish(monitor, 'stopped');
}

function handleEntry(id, fields) {
  const { clientId } = fields;
  if (!clientId) return redis.xack(K.healthCheckStream, HEALTH_GROUP, id);

  const existing = monitors.get(clientId);
  if (existing) {
    if (!existing.entryIds.includes(id)) existing.entryIds.push(id);
    return undefined;
  }
  const monitor = { clientId, entryIds: [id], startedAt: Date.now(), probes: 0, consecutiveOk: 0, controller: new AbortController() };
  monitors.set(clientId, monitor);
  emit('HEALTH_MONITOR_STARTED', {
    clientId,
    streamId: id,
    msg: `XREADGROUP picked up "check ${clientId} status" (${id}) — probing ${healthUrl(clientId).replace(/^https?:\/\/[^/]+/, '')}`,
  });
  runMonitor(monitor).catch((err) => {
    log.error(`monitor for ${clientId} failed`, { error: err.message });
    if (monitors.get(clientId) === monitor) monitors.delete(clientId);
  });
  return undefined;
}

async function readLoop() {
  // Start with entries delivered to this consumer before a restart ("0"), then new ones (">").
  let cursor = '0';
  while (running) {
    try {
      const response = await reader.xreadgroup(
        'GROUP', HEALTH_GROUP, CONSUMER_NAME, 'COUNT', 20, 'BLOCK', 5000, 'STREAMS', K.healthCheckStream, cursor,
      );
      const entries = response?.[0]?.[1] ?? [];
      if (cursor !== '>') cursor = entries.length ? entries[entries.length - 1][0] : '>';
      for (const [id, fields] of entries) {
        // Entries trimmed from the stream come back from the history with null fields.
        if (!fields) await redis.xack(K.healthCheckStream, HEALTH_GROUP, id);
        else handleEntry(id, toObject(fields));
      }
    } catch (err) {
      if (!running) break;
      if (String(err.message).includes('NOGROUP')) await ensureGroup();
      else log.warn('stream read failed', { error: err.message });
      await sleep(1000);
    }
  }
}

/** Takes over events left pending by a crashed health checker instance. */
async function claimOrphans() {
  const [, entries] = await redis.xautoclaim(K.healthCheckStream, HEALTH_GROUP, CONSUMER_NAME, CLAIM_IDLE_MS, '0-0', 'COUNT', 50);
  for (const [id, fields] of entries ?? []) if (fields) handleEntry(id, toObject(fields));
}

const app = createHttpApp();
app.get('/healthz', () => ({ ok: true, monitors: monitors.size }));
app.get('/monitors', () => [...monitors.values()].map((m) => ({ clientId: m.clientId, probes: m.probes, consecutiveOk: m.consecutiveOk })));

async function main() {
  await app.listen(PORTS.healthChecker);
  await ensureGroup();
  await redis.del(K.monitors);

  await sub.subscribe(CH.control);
  sub.on('message', (_channel, raw) => {
    if (JSON.parse(raw).type !== 'RESET') return;
    for (const monitor of monitors.values()) monitor.controller.abort();
  });

  setInterval(() => claimOrphans().catch((err) => log.warn('xautoclaim failed', { error: err.message })), 15_000).unref();
  log.info(`health checker ${CONSUMER_NAME} reading ${K.healthCheckStream} (group ${HEALTH_GROUP})`);
  await readLoop();
}

onShutdown(log, async () => {
  running = false;
  for (const monitor of monitors.values()) monitor.controller.abort();
  reader.disconnect();
  redis.disconnect();
  sub.disconnect();
});

main().catch((err) => {
  log.error('failed to start', { error: err.message });
  process.exit(1);
});
