// Builds the dashboard snapshot from Redis, Kafka (topic and group offsets)
// and the mock merchants' control API.
import { CLIENTS, CONSUMER_GROUP, MERCHANT_URL, TOPIC } from '../shared/config.js';
import { requestJson } from '../shared/http.js';
import { HEALTH_GROUP, K } from '../shared/keys.js';
import { parseSettings } from '../shared/redis.js';

const TXN_LIMIT = 40;
const TXN_FIELDS = [
  'txnId', 'clientId', 'state', 'attempts', 'totalAttempts', 'lastResult', 'producedAt',
  'deliveredAt', 'partition', 'offset', 'amount', 'type', 'nextAttemptAt',
];
const STAT_FIELDS = ['produced', 'delivered', 'attempts', 'failures', 'rateLimited', 'deadLettered', 'markedDown', 'paced'];

const parseJson = (value) => {
  try {
    return value ? JSON.parse(value) : null;
  } catch {
    return null;
  }
};

export function createStateCollector({ redis, kafka, log }) {
  const admin = kafka.admin();
  let adminConnected = false;
  let offsets = { partitions: [], updatedAt: 0, error: 'not loaded yet' };
  let merchants = { clients: [], error: 'not loaded yet' };

  async function refreshOffsets() {
    try {
      if (!adminConnected) {
        await admin.connect();
        adminConnected = true;
      }
      const [topicOffsets, groupOffsets] = await Promise.all([
        admin.fetchTopicOffsets(TOPIC),
        admin.fetchOffsets({ groupId: CONSUMER_GROUP, topics: [TOPIC] }),
      ]);
      const committed = new Map((groupOffsets[0]?.partitions ?? []).map((p) => [p.partition, Number(p.offset)]));
      offsets = {
        updatedAt: Date.now(),
        error: null,
        partitions: topicOffsets
          .map((p) => {
            const low = Number(p.low);
            const high = Number(p.high);
            const commit = committed.get(p.partition) ?? -1;
            return { partition: p.partition, low, high, committed: commit, lag: Math.max(0, high - (commit >= 0 ? commit : low)) };
          })
          .sort((a, b) => a.partition - b.partition),
      };
    } catch (err) {
      if (offsets.error !== err.message) log.warn('could not read Kafka offsets', { error: err.message });
      offsets = { ...offsets, error: err.message };
    }
  }

  async function refreshMerchants() {
    try {
      merchants = { ...(await requestJson(`${MERCHANT_URL}/control`, { timeoutMs: 1500 })), error: null };
    } catch (err) {
      merchants = { ...merchants, error: err.message };
    }
  }

  async function collect() {
    await Promise.all([refreshOffsets(), refreshMerchants()]);

    const pipeline = redis.pipeline();
    for (const client of CLIENTS) {
      pipeline.get(K.clientStatus(client.id));
      pipeline.pttl(K.clientThrottle(client.id));
      pipeline.get(K.clientHealthCheck(client.id));
      pipeline.hgetall(K.clientStats(client.id));
      pipeline.get(K.clientRateLimit(client.id));
      pipeline.get(K.clientAdaptiveRate(client.id));
    }
    pipeline.hgetall(K.holds);
    pipeline.hgetall(K.monitors);
    pipeline.hgetall(K.settings);
    pipeline.zrevrange(K.recentTxns, 0, TXN_LIMIT - 1);
    pipeline.zcard(K.recentTxns);
    pipeline.xlen(K.healthCheckStream);
    pipeline.xpending(K.healthCheckStream, HEALTH_GROUP);
    pipeline.get(K.epoch);
    const results = (await pipeline.exec()).map(([err, value]) => (err ? null : value));

    let i = 0;
    const clients = CLIENTS.map((client) => {
      const [status, throttlePttl, healthFlag, stats, contracted, adaptive] = results.slice(i, (i += 6));
      const mock = merchants.clients?.find((m) => m.id === client.id) ?? null;
      return {
        ...client,
        status: status ?? 'UP',
        throttleMs: throttlePttl > 0 ? throttlePttl : 0,
        healthCheckPending: Boolean(healthFlag),
        contractedRate: Number(contracted) || client.rateLimitPerSec,
        adaptiveRate: Number(adaptive) || null,
        stats: Object.fromEntries(STAT_FIELDS.map((field) => [field, Number(stats?.[field] ?? 0)])),
        mock,
      };
    });
    const [holds, monitors, settings, txnIds, txnCount, streamLength, pending, epoch] = results.slice(i);

    const txnPipeline = redis.pipeline();
    for (const id of txnIds ?? []) txnPipeline.hmget(K.txn(id), ...TXN_FIELDS);
    const txns = (await txnPipeline.exec())
      .map(([err, values]) => (err ? null : Object.fromEntries(TXN_FIELDS.map((field, index) => [field, values[index]]))))
      .filter((txn) => txn?.txnId)
      .map((txn) => ({
        ...txn,
        attempts: Number(txn.attempts ?? 0),
        totalAttempts: Number(txn.totalAttempts ?? 0),
        producedAt: Number(txn.producedAt) || null,
        deliveredAt: Number(txn.deliveredAt) || null,
        nextAttemptAt: Number(txn.nextAttemptAt) || null,
        partition: txn.partition === null ? null : Number(txn.partition),
        offset: txn.offset === null ? null : Number(txn.offset),
        amount: Number(txn.amount),
      }));

    const holdByPartition = Object.fromEntries(Object.entries(holds ?? {}).map(([p, v]) => [p, parseJson(v)]));
    return {
      ts: Date.now(),
      epoch: Number(epoch) || 0,
      settings: parseSettings(settings ?? {}),
      clients,
      partitions: CLIENTS.map((client) => {
        const offset = offsets.partitions.find((p) => p.partition === client.partition);
        return {
          partition: client.partition,
          clientId: client.id,
          low: offset?.low ?? 0,
          high: offset?.high ?? 0,
          committed: offset?.committed ?? -1,
          lag: offset?.lag ?? 0,
          hold: holdByPartition[client.partition] ?? null,
        };
      }),
      monitors: Object.values(monitors ?? {}).map(parseJson).filter(Boolean),
      healthStream: { length: Number(streamLength ?? 0), pending: Number(pending?.[0] ?? 0) },
      txns,
      txnCount: Number(txnCount ?? 0),
      services: {
        kafka: !offsets.error,
        merchants: !merchants.error,
        kafkaError: offsets.error,
        merchantsError: merchants.error,
      },
    };
  }

  return { collect, disconnect: () => (adminConnected ? admin.disconnect() : undefined) };
}
