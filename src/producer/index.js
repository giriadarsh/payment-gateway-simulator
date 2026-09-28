// Payment gateway (producer). Creates transactions, stores each notification
// payload in Redis and publishes only a reference ({ txnId }) to Kafka, keyed
// by client id so it lands on the client's dedicated partition.
import { getClient, PORTS, TOPIC } from '../shared/config.js';
import { createHttpApp, HttpError } from '../shared/http.js';
import { createKafka, ensureTopic } from '../shared/kafka.js';
import { CH, K } from '../shared/keys.js';
import { createEventPublisher, createRedis } from '../shared/redis.js';
import { clamp, createLogger, newId, onShutdown, pick, sleep } from '../shared/util.js';
import { ClientPartitioner } from './partitioner.js';

const TXN_TTL_SECONDS = 24 * 60 * 60;
const METHODS = ['UPI', 'CARD', 'NETBANKING', 'WALLET'];

const log = createLogger('producer');
const redis = createRedis('producer');
const sub = createRedis('producer-sub');
const emit = createEventPublisher(redis, 'producer');
const kafka = createKafka('payment-gateway');
const producer = kafka.producer({ createPartitioner: ClientPartitioner, allowAutoTopicCreation: false });

/** In-flight batches, cancelled when the simulation is reset. */
const activeBatches = new Set();
let ready = false;

function buildNotification(clientId) {
  const type = Math.random() < 0.15 ? 'payment.failed' : 'payment.captured';
  const txnId = newId('txn');
  const amount = Math.round((100 + Math.random() * 24900) * 100) / 100;
  const notification = {
    id: newId('evt'),
    type,
    createdAt: new Date().toISOString(),
    data: {
      transactionId: txnId,
      orderId: newId('order'),
      clientId,
      amount,
      currency: 'INR',
      method: pick(METHODS),
      status: type === 'payment.failed' ? 'FAILED' : 'CAPTURED',
    },
  };
  return { txnId, notification };
}

async function publishTransaction(clientId) {
  const { txnId, notification } = buildNotification(clientId);
  const epoch = Number(await redis.get(K.epoch)) || 0;
  const now = Date.now();

  // 1. The payload goes to Redis first, so the consumer can always resolve the reference.
  await redis
    .multi()
    .hset(K.txn(txnId), {
      txnId,
      eventId: notification.id,
      clientId,
      type: notification.type,
      amount: notification.data.amount,
      payload: JSON.stringify(notification),
      state: 'PENDING',
      attempts: 0,
      totalAttempts: 0,
      producedAt: now,
      epoch,
    })
    .expire(K.txn(txnId), TXN_TTL_SECONDS)
    .zadd(K.recentTxns, now, txnId)
    .zremrangebyrank(K.recentTxns, 0, -501)
    .hincrby(K.clientStats(clientId), 'produced', 1)
    .exec();
  emit('TXN_STORED', {
    txnId,
    clientId,
    level: 'debug',
    msg: `HSET ${K.txn(txnId)} — payload (${notification.type}, INR ${notification.data.amount}) stored in Redis`,
  });

  // 2. Kafka carries only the reference. The key (client id) selects the partition.
  const [record] = await producer.send({
    topic: TOPIC,
    messages: [
      {
        key: clientId,
        value: JSON.stringify({ txnId, eventId: notification.id, clientId, type: notification.type, epoch, producedAt: now }),
        headers: { 'client-id': clientId },
      },
    ],
  });
  const partition = record.partition;
  const offset = Number(record.baseOffset);
  await redis.hset(K.txn(txnId), { partition, offset });
  emit('MESSAGE_PRODUCED', {
    txnId,
    clientId,
    partition,
    offset,
    msg: `Published ${txnId} to ${TOPIC} partition ${partition} (key ${clientId}, offset ${offset})`,
  });
  return txnId;
}

const app = createHttpApp();

app.get('/healthz', () => {
  if (!ready) throw new HttpError(503, 'starting');
  return { ok: true };
});

/** POST /transactions { clientId, count?, intervalMs? } */
app.post('/transactions', async ({ body }) => {
  const clientId = body?.clientId;
  if (!getClient(clientId)) throw new HttpError(400, `Unknown clientId "${clientId}"`);
  if (!ready) throw new HttpError(503, 'Producer is still connecting to Kafka');
  const count = clamp(Math.floor(Number(body.count ?? 1)) || 1, 1, 200);
  const intervalMs = clamp(Number(body.intervalMs ?? 0) || 0, 0, 10_000);

  const controller = new AbortController();
  activeBatches.add(controller);
  const txnIds = [];
  try {
    for (let i = 0; i < count && !controller.signal.aborted; i += 1) {
      if (i > 0 && intervalMs > 0) await sleep(intervalMs, controller.signal);
      if (controller.signal.aborted) break;
      txnIds.push(await publishTransaction(clientId));
    }
  } finally {
    activeBatches.delete(controller);
  }
  return { clientId, txnIds, cancelled: controller.signal.aborted };
});

async function main() {
  await app.listen(PORTS.producer);
  await ensureTopic(kafka, log);
  await producer.connect();

  await sub.subscribe(CH.control);
  sub.on('message', (_channel, raw) => {
    if (JSON.parse(raw).type !== 'RESET') return;
    for (const controller of activeBatches) controller.abort();
  });

  ready = true;
  log.info(`payment gateway listening on :${PORTS.producer}, publishing to ${TOPIC}`);
}

onShutdown(log, async () => {
  await producer.disconnect();
  redis.disconnect();
  sub.disconnect();
});

main().catch((err) => {
  log.error('failed to start', { error: err.message });
  process.exit(1);
});
