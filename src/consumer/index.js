// Notification consumer. Reads notification references from Kafka, checks the
// client's status in Redis, fetches the payload from Redis and calls the
// client's webhook. Messages that cannot be delivered yet stay unacknowledged
// while only that client's partition is paused.
import { CONSUMER_GROUP, PARTITIONS, PORTS, TOPIC } from '../shared/config.js';
import { createHttpApp, HttpError } from '../shared/http.js';
import { createKafka, ensureTopic } from '../shared/kafka.js';
import { CH, K } from '../shared/keys.js';
import { createEventPublisher, createRedis, createSettingsReader } from '../shared/redis.js';
import { createLogger, formatMs, onShutdown } from '../shared/util.js';
import { createDeliveryProcessor } from './delivery.js';
import { PartitionWorker } from './partition-worker.js';
import { createRateLimiter } from './rate-limiter.js';

const RECONCILE_INTERVAL_MS = 3000;

const log = createLogger('consumer');
const redis = createRedis('consumer');
const sub = createRedis('consumer-sub');
const emit = createEventPublisher(redis, 'consumer');
const getSettings = createSettingsReader(redis);
const limiter = createRateLimiter(redis);
const { processMessage, requestHealthCheck } = createDeliveryProcessor({ redis, emit, getSettings, limiter });

const kafka = createKafka('notification-consumer');
const consumer = kafka.consumer({
  groupId: CONSUMER_GROUP,
  allowAutoTopicCreation: false,
  maxWaitTimeInMs: 100, // resumed partitions are fetched again quickly
  sessionTimeout: 30_000,
  heartbeatInterval: 3000,
});

/** partition -> PartitionWorker */
const workers = new Map();
let assigned = [];
let ready = false;

function holdMessage(hold) {
  const p = `Partition ${hold.partition} paused`;
  switch (hold.reason) {
    case 'CLIENT_DOWN':
      return `${p} — ${hold.txnId} left unacknowledged until ${hold.clientId} is UP again`;
    case 'BACKOFF':
      return `${p} for ${formatMs(hold.resumeInMs)} (backoff before attempt ${hold.attempt})`;
    case 'RATE_LIMITED':
      return `${p} for ${formatMs(hold.resumeInMs)} (honouring Retry-After)`;
    default:
      return `${p} (${hold.reason})`;
  }
}

function publishHold(hold) {
  const view = {
    partition: hold.partition,
    clientId: hold.clientId,
    txnId: hold.txnId,
    offset: hold.offset,
    reason: hold.reason,
    since: hold.since,
    until: hold.until,
    attempt: hold.attempt,
    rate: hold.rate,
  };
  redis.hset(K.holds, String(hold.partition), JSON.stringify(view)).catch(() => {});
  if (!hold.quiet) emit('PARTITION_HELD', { ...view, msg: holdMessage(hold) });
}

function publishRelease(hold, trigger) {
  redis.hdel(K.holds, String(hold.partition)).catch(() => {});
  if (hold.quiet || trigger === 'stopped') return;
  const why = { timer: 'wait is over', 'status-up': `${hold.clientId} is UP`, reconcile: `${hold.clientId} is UP`, reset: 'simulation reset' }[trigger] ?? trigger;
  emit('PARTITION_RESUMED', {
    partition: hold.partition,
    clientId: hold.clientId,
    txnId: hold.txnId,
    reason: hold.reason,
    trigger,
    level: hold.reason === 'CLIENT_DOWN' ? 'info' : 'debug',
    msg: `Partition ${hold.partition} resumed (${why}) — re-evaluating ${hold.txnId}`,
  });
}

function workerFor(partition) {
  let worker = workers.get(partition);
  if (worker) return worker;
  worker = new PartitionWorker({
    partition,
    log,
    processMessage,
    commit: async (p, offset, decision) => {
      const next = (BigInt(offset) + 1n).toString();
      await consumer.commitOffsets([{ topic: TOPIC, partition: p, offset: next }]);
      if (!decision.stale) {
        emit('OFFSET_COMMITTED', {
          partition: p,
          offset: Number(offset),
          txnId: decision.txnId,
          clientId: decision.clientId,
          level: 'debug',
          msg: `Committed offset ${next} on partition ${p} — ${decision.txnId} acknowledged`,
        });
      }
    },
    onDrained: (p) => consumer.resume([{ topic: TOPIC, partitions: [p] }]),
    onHold: publishHold,
    onRelease: publishRelease,
  });
  workers.set(partition, worker);
  return worker;
}

function dropWorkers(filter = () => true) {
  for (const [partition, worker] of workers) {
    if (!filter(partition)) continue;
    worker.stop();
    workers.delete(partition);
    redis.hdel(K.holds, String(partition)).catch(() => {});
  }
}

/** Release every CLIENT_DOWN hold for a client that is UP again. */
function releaseClient(clientId, trigger) {
  for (const worker of workers.values()) {
    if (worker.hold?.reason === 'CLIENT_DOWN' && worker.hold.clientId === clientId) worker.release(trigger);
  }
}

/**
 * Safety net for missed pub/sub messages: resume holds whose client is UP, and
 * re-request a health check if the pending flag expired (e.g. checker restarted).
 */
async function reconcile() {
  for (const worker of workers.values()) {
    const hold = worker.hold;
    if (hold?.reason !== 'CLIENT_DOWN') continue;
    const status = await redis.get(K.clientStatus(hold.clientId));
    if (status !== 'DOWN') worker.release('reconcile');
    else await requestHealthCheck(hold.clientId, 'reconcile', { clientId: hold.clientId, partition: hold.partition });
  }
}

const app = createHttpApp();
app.get('/healthz', () => {
  if (!ready) throw new HttpError(503, 'not in the consumer group yet');
  return { ok: true, assigned };
});
app.get('/workers', () =>
  [...workers.values()].map((w) => ({
    partition: w.partition,
    queued: w.queue.length,
    running: w.running,
    hold: w.hold && { reason: w.hold.reason, txnId: w.hold.txnId, until: w.hold.until },
  })),
);

async function main() {
  await app.listen(PORTS.consumer);
  await ensureTopic(kafka, log);

  await sub.subscribe(CH.clientStatus, CH.control);
  sub.on('message', (channel, raw) => {
    const msg = JSON.parse(raw);
    if (channel === CH.clientStatus && msg.status === 'UP') releaseClient(msg.clientId, 'status-up');
    if (channel === CH.control && msg.type === 'RESET') {
      for (const worker of workers.values()) worker.release('reset');
    }
  });

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    assigned = payload.memberAssignment[TOPIC] ?? [];
    dropWorkers((partition) => !assigned.includes(partition));
    // Pause state survives rebalances: make sure idle partitions are fetchable.
    const idle = assigned.filter((partition) => !workers.get(partition)?.running);
    if (idle.length) consumer.resume([{ topic: TOPIC, partitions: idle }]);
    ready = true;
    emit('CONSUMER_JOINED', {
      partitions: assigned,
      level: 'debug',
      msg: `Joined consumer group ${CONSUMER_GROUP}; assigned partitions ${assigned.join(', ')}`,
    });
    log.info(`joined ${CONSUMER_GROUP}, partitions [${assigned.join(', ')}]`);
  });

  consumer.on(consumer.events.CRASH, ({ payload }) => {
    log.error('consumer crashed', { error: payload.error?.message, restart: payload.restart });
    // A restarted consumer starts from committed offsets; in-memory batches are stale.
    dropWorkers();
    ready = false;
    if (!payload.restart) process.exit(1);
  });

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    partitionsConsumedConcurrently: PARTITIONS,
    eachBatch: async ({ batch, resolveOffset }) => {
      if (!batch.messages.length) return;
      consumer.pause([{ topic: batch.topic, partitions: [batch.partition] }]);
      workerFor(batch.partition).enqueue(batch.messages, resolveOffset);
    },
  });

  setInterval(() => reconcile().catch((err) => log.warn('reconcile failed', { error: err.message })), RECONCILE_INTERVAL_MS).unref();
  log.info(`consumer running (group ${CONSUMER_GROUP}, topic ${TOPIC})`);
}

onShutdown(log, async () => {
  dropWorkers();
  await consumer.disconnect();
  redis.disconnect();
  sub.disconnect();
});

main().catch((err) => {
  log.error('failed to start', { error: err.message });
  process.exit(1);
});
