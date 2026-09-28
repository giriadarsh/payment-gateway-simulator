// Scenario definitions and the runner that orchestrates them. A scenario only
// sets up conditions (mock client behaviour, Redis status, traffic) and then
// observes; all delivery decisions are made by the real services.
import { CLIENTS, getClient, MERCHANT_URL, PRODUCER_URL, RATE_LIMIT_STRATEGIES } from '../shared/config.js';
import { HttpError, requestJson } from '../shared/http.js';
import { K } from '../shared/keys.js';
import { parseSettings } from '../shared/redis.js';
import { sleep } from '../shared/util.js';

const FAILURE_OPTIONS = [
  { value: 'HTTP_503', label: 'HTTP 503' },
  { value: 'TIMEOUT', label: 'Timeout (no response)' },
  { value: 'CONNECTION_RESET', label: 'Connection reset' },
];

const STRATEGY_TEXT = {
  reactive:
    'Reactive: send as fast as possible; on 429 store client:{clientId}:throttle with a TTL of Retry-After and pause the partition. The retry budget is untouched and the client is never marked DOWN.',
  'token-bucket':
    'Token bucket: before each call take a token from a Redis bucket refilled at 90% of the contracted {limitPerSec} req/s (a margin for jitter). The consumer paces itself instead of collecting 429s.',
  adaptive:
    'Adaptive (AIMD): start at a high rate, halve it on every 429 and add 0.25 req/s per success, converging on the limit without knowing it.',
};

/** A healthy client to contrast with the one under test (the first other registered client). */
const otherClientId = (clientId) => CLIENTS.find((client) => client.id !== clientId).id;

export const SCENARIOS = [
  {
    id: 'happy-path',
    number: '1',
    title: 'Client up: delivered',
    summary: 'Status is UP, the payload comes from Redis, the webhook returns 200 and the offset is committed.',
    params: [
      { key: 'clientId', label: 'Client', type: 'client', default: 'amazon' },
      { key: 'count', label: 'Transactions', type: 'number', default: 5, min: 1, max: 30 },
    ],
    steps: [
      'Client webhook is healthy and client:{clientId}:status = UP',
      'Gateway stores each payload in Redis (txn:{id}) and publishes the txn id to partition {partition}',
      'Consumer checks client:{clientId}:status → UP, then reads the payload from Redis',
      'Webhook answers 200 → consumer commits the offset (message acknowledged)',
    ],
    async run(ctx) {
      const { clientId, count } = ctx.params;
      ctx.step(0);
      await ctx.behave(clientId, { mode: 'HEALTHY' });
      await ctx.setStatus(clientId, 'UP');
      await ctx.sleep(600);
      ctx.step(1);
      const producing = ctx.background(ctx.produce(clientId, count, 400));
      await ctx.sleep(600);
      ctx.step(2);
      await producing;
      ctx.step(3);
      await ctx.waitDelivered();
      return ctx.summary();
    },
  },
  {
    id: 'client-down',
    number: '2',
    title: 'Client DOWN: hold and health check',
    summary:
      'Redis already says DOWN. The consumer leaves the message unacknowledged, pauses only that partition and asks the health checker to watch the client.',
    params: [
      { key: 'clientId', label: 'Client', type: 'client', default: 'flipkart' },
      { key: 'count', label: 'Transactions', type: 'number', default: 4, min: 1, max: 30 },
      { key: 'recoverAfterSec', label: 'Client recovers after (s)', type: 'number', default: 12, min: 4, max: 120 },
    ],
    steps: [
      'Outage: the webhook is down and client:{clientId}:status = DOWN in Redis',
      'Consumer reads the message, sees DOWN: no webhook call, offset not committed, partition {partition} paused',
      'Consumer XADDs a "check client status" event; the health checker picks it up and probes /health',
      'Other clients keep flowing: {otherClient} is delivered while {clientId} is paused',
      'Client recovers → health checker sets status UP and publishes the change',
      'Consumer resumes partition {partition} and delivers the backlog in order',
    ],
    async run(ctx) {
      const { clientId, count, recoverAfterSec } = ctx.params;
      const other = otherClientId(clientId);
      ctx.step(0);
      await ctx.behave(other, { mode: 'HEALTHY' });
      await ctx.setStatus(other, 'UP');
      await ctx.behave(clientId, { mode: 'DOWN', failureStyle: 'HTTP_503', recoverAfterMs: recoverAfterSec * 1000 });
      await ctx.setStatus(clientId, 'DOWN');
      await ctx.sleep(800);
      ctx.step(1);
      await ctx.produce(clientId, count, 300);
      ctx.step(2);
      await ctx.waitFor(() => ctx.redis.hexists(K.monitors, clientId), { label: 'the health checker to start probing', timeoutMs: 20_000 });
      ctx.step(3);
      await ctx.produce(other, 3, 300);
      ctx.step(4);
      await ctx.waitFor(async () => (await ctx.status(clientId)) === 'UP', {
        label: `${clientId} to be marked UP`,
        timeoutMs: (recoverAfterSec + 60) * 1000,
      });
      ctx.step(5);
      await ctx.waitDelivered();
      return ctx.summary();
    },
  },
  {
    id: 'backoff-then-down',
    number: '3',
    title: 'Client failing: backoff, then DOWN',
    summary:
      'Redis says UP but calls fail. The consumer retries with exponential backoff; when the budget runs out it marks the client DOWN and requests a health check.',
    params: [
      { key: 'clientId', label: 'Client', type: 'client', default: 'flipkart' },
      { key: 'count', label: 'Transactions', type: 'number', default: 3, min: 1, max: 30 },
      { key: 'failureStyle', label: 'Failure', type: 'select', default: 'HTTP_503', options: FAILURE_OPTIONS },
      { key: 'recoverAfterSec', label: 'Recovers N s after marked DOWN', type: 'number', default: 8, min: 2, max: 120 },
    ],
    steps: [
      'client:{clientId}:status is UP, but the webhook starts failing ({failureStyle})',
      'Attempt 1 fails → exponential backoff ({backoffPlan}) while partition {partition} waits; other clients are unaffected',
      'All {maxAttempts} attempts failed → consumer sets client:{clientId}:status = DOWN and XADDs a health-check event',
      'Health checker probes /health until {healthyThreshold} consecutive successes',
      'Status UP → partition resumed → the held notification is delivered with a fresh retry budget',
    ],
    async run(ctx) {
      const { clientId, count, failureStyle, recoverAfterSec } = ctx.params;
      ctx.step(0);
      await ctx.setStatus(clientId, 'UP');
      await ctx.behave(clientId, { mode: 'DOWN', failureStyle });
      await ctx.sleep(600);
      ctx.step(1);
      await ctx.produce(clientId, count, 250);
      await ctx.waitFor(async () => (await ctx.status(clientId)) === 'DOWN', { label: `${clientId} to be marked DOWN`, timeoutMs: 180_000 });
      ctx.step(2);
      await ctx.behave(clientId, { recoverAfterMs: recoverAfterSec * 1000 });
      await ctx.waitFor(() => ctx.redis.hexists(K.monitors, clientId), { label: 'the health checker to start probing', timeoutMs: 20_000 });
      ctx.step(3);
      await ctx.waitFor(async () => (await ctx.status(clientId)) === 'UP', {
        label: `${clientId} to be marked UP`,
        timeoutMs: (recoverAfterSec + 60) * 1000,
      });
      ctx.step(4);
      await ctx.waitDelivered();
      return ctx.summary();
    },
  },
  {
    id: 'rate-limited',
    number: '4',
    title: 'Client rate limited (HTTP 429)',
    summary:
      'The client accepts only N requests/sec. Compare three strategies: honour Retry-After, pace with a token bucket, or discover the limit adaptively.',
    params: [
      { key: 'clientId', label: 'Client', type: 'client', default: 'blinkit' },
      { key: 'count', label: 'Burst size', type: 'number', default: 20, min: 5, max: 60 },
      { key: 'limitPerSec', label: 'Client limit (req/s)', type: 'number', default: 2, min: 1, max: 10 },
      {
        key: 'strategy',
        label: 'Strategy',
        type: 'select',
        default: 'reactive',
        options: [
          { value: 'reactive', label: 'Reactive: honour Retry-After' },
          { value: 'token-bucket', label: 'Token bucket: known limit' },
          { value: 'adaptive', label: 'Adaptive: AIMD' },
        ],
      },
    ],
    steps: [
      '{clientId} accepts {limitPerSec} req/s and answers 429 with Retry-After: 1 above that',
      'A burst of {count} notifications arrives for {clientId}',
      '{strategyText}',
      'Everything is delivered; compare the 429 count and total time across strategies',
    ],
    async run(ctx) {
      const { clientId, count, limitPerSec, strategy } = ctx.params;
      ctx.step(0);
      await ctx.setStatus(clientId, 'UP');
      await ctx.behave(clientId, { mode: 'RATE_LIMITED', rateLimitPerSec: limitPerSec, retryAfterSec: 1 });
      await ctx.updateSettings({ rateLimitStrategy: strategy });
      await ctx.redis
        .multi()
        .set(K.clientRateLimit(clientId), String(limitPerSec))
        .del(K.clientAdaptiveRate(clientId), K.clientBucket(clientId), K.clientThrottle(clientId))
        .exec();
      await ctx.sleep(1200); // let the client's own bucket refill
      const before = await ctx.stats(clientId);
      ctx.step(1);
      await ctx.produce(clientId, count, 25);
      ctx.step(2);
      await ctx.waitDelivered(undefined, 240_000);
      const after = await ctx.stats(clientId);
      ctx.step(3);
      const delta = (field) => Number(after[field] ?? 0) - Number(before[field] ?? 0);
      return ctx.summary({ strategy, http429: delta('rateLimited'), pacingWaits: delta('paced') });
    },
  },
  {
    id: 'transient',
    number: '5',
    title: 'Transient blip: backoff succeeds',
    summary: 'The webhook fails a couple of times and recovers within the retry budget, so the client is never marked DOWN.',
    params: [
      { key: 'clientId', label: 'Client', type: 'client', default: 'flipkart' },
      { key: 'failures', label: 'Failed requests', type: 'number', default: 2, min: 1, max: 6 },
      { key: 'failureStyle', label: 'Failure', type: 'select', default: 'HTTP_503', options: FAILURE_OPTIONS },
    ],
    steps: [
      'The next {failures} webhook requests to {clientId} fail ({failureStyle})',
      'Each failure schedules a retry with exponential backoff ({backoffPlan}); the partition waits',
      'The next attempt succeeds → offset committed; status stays UP and no health check is needed',
    ],
    async run(ctx) {
      const { clientId, failures, failureStyle } = ctx.params;
      ctx.step(0);
      await ctx.setStatus(clientId, 'UP');
      await ctx.behave(clientId, { mode: 'FLAKY', failNext: failures, failureStyle });
      await ctx.sleep(600);
      ctx.step(1);
      await ctx.produce(clientId, 2, 300);
      await ctx.waitDelivered(undefined, 120_000);
      ctx.step(2);
      return ctx.summary();
    },
  },
  {
    id: 'isolation',
    number: '6',
    title: 'Everything at once: isolation',
    summary: 'One healthy, one failing and one rate-limited client at the same time. Every lane only affects itself.',
    params: [{ key: 'count', label: 'Transactions per client', type: 'number', default: 5, min: 1, max: 15 }],
    steps: [
      'Amazon healthy · Flipkart failing (backoff, then DOWN) · Blinkit limited to 2 req/s',
      '{count} notifications per client are produced concurrently',
      'Each client has its own partition, so a paused lane never blocks another one',
      'Flipkart recovers and drains its backlog; Blinkit is delivered at its own pace',
    ],
    async run(ctx) {
      const { count } = ctx.params;
      ctx.step(0);
      await ctx.behave('amazon', { mode: 'HEALTHY' });
      await ctx.behave('flipkart', { mode: 'DOWN', failureStyle: 'HTTP_503' });
      await ctx.behave('blinkit', { mode: 'RATE_LIMITED', rateLimitPerSec: 2, retryAfterSec: 1 });
      for (const client of CLIENTS) await ctx.setStatus(client.id, 'UP');
      await ctx.sleep(800);
      ctx.step(1);
      const recovery = ctx.background(ctx.recoverAfterMarkedDown('flipkart', 8000));
      // Blinkit gets a quick burst so its rate limit actually kicks in.
      await Promise.all(CLIENTS.map((client) => ctx.produce(client.id, count, client.id === 'blinkit' ? 80 : 350)));
      ctx.step(2);
      await ctx.waitDelivered(ctx.txnIdsFor('amazon'), 60_000);
      ctx.step(3);
      await recovery;
      await ctx.waitDelivered(undefined, 240_000);
      return ctx.summary();
    },
  },
];

/** Metadata for the UI (everything except the run functions). */
export const scenarioCatalog = () =>
  SCENARIOS.map(({ id, number, title, summary, params, steps }) => ({ id, number, title, summary, params, steps, strategyText: STRATEGY_TEXT }));

function coerceParams(scenario, input = {}) {
  const params = {};
  for (const spec of scenario.params) {
    const raw = input[spec.key] ?? spec.default;
    if (spec.type === 'number') {
      const value = Number(raw);
      if (!Number.isFinite(value)) throw new HttpError(400, `${spec.label} must be a number`);
      params[spec.key] = Math.min(spec.max, Math.max(spec.min, Math.round(value)));
    } else if (spec.type === 'client') {
      if (!getClient(raw)) throw new HttpError(400, `Unknown client "${raw}"`);
      params[spec.key] = raw;
    } else if (spec.type === 'select') {
      if (!spec.options.some((option) => option.value === raw)) throw new HttpError(400, `Invalid ${spec.label}`);
      params[spec.key] = raw;
    }
  }
  if (params.strategy && !RATE_LIMIT_STRATEGIES.includes(params.strategy)) throw new HttpError(400, 'Invalid strategy');
  return params;
}

function describeBackoff(settings) {
  const delays = [];
  for (let attempt = 1; attempt < settings.maxAttempts && delays.length < 4; attempt += 1) {
    const ms = Math.min(settings.maxBackoffMs, settings.baseBackoffMs * 2 ** (attempt - 1));
    delays.push(ms < 1000 ? `${ms} ms` : `${ms / 1000} s`);
  }
  return `${delays.join(', ')}${settings.maxAttempts > 5 ? ' …' : ''}`;
}

export const fillTemplate = (text, vars) => text.replace(/\{(\w+)\}/g, (match, key) => (key in vars ? String(vars[key]) : match));

export function createScenarioRunner({ redis, emit, log, setClientStatus, updateSettings }) {
  let current = null;

  function publicState() {
    if (!current) return null;
    const { id, number, title, params, steps, stepIndex, status, startedAt, finishedAt, summary, error } = current;
    return { id, number, title, params, steps, stepIndex, status, startedAt, finishedAt, summary, error };
  }

  function cancel(reason = 'cancelled') {
    if (current?.status === 'running') {
      current.status = 'cancelled';
      current.controller.abort(new Error(reason));
    }
  }

  async function start(id, input) {
    const scenario = SCENARIOS.find((s) => s.id === id);
    if (!scenario) throw new HttpError(404, `Unknown scenario "${id}"`);
    const params = coerceParams(scenario, input);
    cancel('superseded by a new scenario');

    const settings = parseSettings(await redis.hgetall(K.settings));
    const client = getClient(params.clientId);
    const vars = {
      ...params,
      partition: client?.partition ?? '',
      otherClient: getClient(otherClientId(params.clientId))?.name,
      maxAttempts: settings.maxAttempts,
      healthyThreshold: settings.healthyThreshold,
      backoffPlan: describeBackoff(settings),
      failureStyle: FAILURE_OPTIONS.find((o) => o.value === params.failureStyle)?.label ?? params.failureStyle,
    };
    vars.strategyText = params.strategy ? fillTemplate(STRATEGY_TEXT[params.strategy], vars) : '';
    const steps = scenario.steps.map((text) => fillTemplate(text, vars));

    const controller = new AbortController();
    const run = {
      id: scenario.id,
      number: scenario.number,
      title: scenario.title,
      params,
      steps,
      stepIndex: -1,
      status: 'running',
      startedAt: Date.now(),
      finishedAt: null,
      summary: null,
      error: null,
      controller,
      txnIds: [],
    };
    current = run;

    const ctx = createContext({ run, redis, emit, setClientStatus, updateSettings });
    emit('SCENARIO_STARTED', { scenarioId: scenario.id, steps, params, msg: `Scenario ${scenario.number} started: ${scenario.title}` });

    scenario
      .run(ctx)
      .then((summary) => {
        if (run.status !== 'running') return;
        run.status = 'finished';
        run.summary = summary;
        emit('SCENARIO_FINISHED', { scenarioId: scenario.id, summary, msg: `Scenario ${scenario.number} finished: ${summaryText(summary)}` });
      })
      .catch((err) => {
        if (run.status === 'cancelled' || controller.signal.aborted) {
          emit('SCENARIO_CANCELLED', { scenarioId: scenario.id, level: 'debug', msg: `Scenario ${scenario.number} cancelled` });
          return;
        }
        run.status = 'failed';
        run.error = err.message;
        log.warn(`scenario ${scenario.id} failed`, { error: err.message });
        emit('SCENARIO_FAILED', { scenarioId: scenario.id, error: err.message, msg: `Scenario ${scenario.number} failed: ${err.message}` });
      })
      .finally(() => {
        run.finishedAt = Date.now();
      });

    return publicState();
  }

  return { start, cancel, current: publicState };
}

function summaryText(summary) {
  const parts = [`${summary.delivered}/${summary.total} delivered in ${(summary.durationMs / 1000).toFixed(1)} s`, `${summary.attempts} webhook calls`];
  if (summary.http429 !== undefined) parts.push(`${summary.http429} × HTTP 429`);
  if (summary.pacingWaits) parts.push(`${summary.pacingWaits} pacing waits`);
  return parts.join(' · ');
}

function createContext({ run, redis, emit, setClientStatus, updateSettings }) {
  const { signal } = run.controller;
  const byClient = new Map();

  const checkAborted = () => {
    if (signal.aborted) throw signal.reason ?? new Error('cancelled');
  };

  const ctx = {
    params: run.params,
    redis,

    step(index) {
      checkAborted();
      run.stepIndex = index;
      emit('SCENARIO_STEP', { scenarioId: run.id, index, text: run.steps[index], msg: `Step ${index + 1}: ${run.steps[index]}` });
    },

    async sleep(ms) {
      await sleep(ms, signal);
      checkAborted();
    },

    async produce(clientId, count, intervalMs) {
      checkAborted();
      const { txnIds } = await requestJson(`${PRODUCER_URL}/transactions`, {
        method: 'POST',
        body: { clientId, count, intervalMs },
        timeoutMs: count * intervalMs + 30_000,
      });
      run.txnIds.push(...txnIds);
      byClient.set(clientId, [...(byClient.get(clientId) ?? []), ...txnIds]);
      return txnIds;
    },

    txnIdsFor: (clientId) => byClient.get(clientId) ?? [],

    /** Marks a promise that is awaited later, so an early rejection is not "unhandled". */
    background(promise) {
      promise.catch(() => {});
      return promise;
    },

    behave: (clientId, behavior) => requestJson(`${MERCHANT_URL}/control/${clientId}`, { method: 'PUT', body: behavior }),

    setStatus: (clientId, status) => setClientStatus(clientId, status, 'scenario'),

    updateSettings,

    status: (clientId) => redis.get(K.clientStatus(clientId)),

    stats: (clientId) => redis.hgetall(K.clientStats(clientId)),

    async waitFor(check, { timeoutMs = 60_000, intervalMs = 300, label = 'condition' } = {}) {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        checkAborted();
        if (await check()) return;
        await sleep(intervalMs, signal);
      }
      throw new Error(`timed out waiting for ${label}`);
    },

    async waitDelivered(ids = run.txnIds, timeoutMs = 120_000) {
      await ctx.waitFor(
        async () => {
          if (!ids.length) return true;
          const states = await Promise.all(ids.map((id) => redis.hget(K.txn(id), 'state')));
          return states.every((state) => state === 'DELIVERED' || state === 'FAILED');
        },
        { timeoutMs, label: 'all notifications to be delivered' },
      );
    },

    /** Lets a failing client recover `afterMs` after the consumer marks it DOWN. */
    async recoverAfterMarkedDown(clientId, afterMs) {
      await ctx.waitFor(async () => (await ctx.status(clientId)) === 'DOWN', { label: `${clientId} to be marked DOWN`, timeoutMs: 180_000 });
      await ctx.behave(clientId, { recoverAfterMs: afterMs });
    },

    async summary(extra = {}) {
      const rows = await Promise.all(
        run.txnIds.map((id) => redis.hmget(K.txn(id), 'state', 'totalAttempts', 'producedAt', 'deliveredAt')),
      );
      const delivered = rows.filter(([state]) => state === 'DELIVERED');
      const attempts = rows.reduce((sum, [, total]) => sum + Number(total ?? 0), 0);
      const firstProduced = Math.min(...rows.map(([, , producedAt]) => Number(producedAt)).filter(Boolean));
      const lastDelivered = Math.max(...delivered.map(([, , , deliveredAt]) => Number(deliveredAt)));
      return {
        total: run.txnIds.length,
        delivered: delivered.length,
        failed: rows.filter(([state]) => state === 'FAILED').length,
        attempts,
        durationMs: delivered.length ? lastDelivered - firstProduced : 0,
        ...extra,
      };
    },
  };
  return ctx;
}
