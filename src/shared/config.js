// Configuration shared by every service. Values come from the environment
// (see docker-compose.yml) with defaults that work for local development.

const env = (name, fallback) => process.env[name] ?? fallback;

export const KAFKA_BROKERS = env('KAFKA_BROKERS', 'localhost:9092').split(',');
export const REDIS_URL = env('REDIS_URL', 'redis://localhost:6379');
export const TOPIC = env('NOTIFICATION_TOPIC', 'payment-notifications');
export const CONSUMER_GROUP = env('CONSUMER_GROUP', 'notification-workers');

export const PRODUCER_URL = env('PRODUCER_URL', 'http://localhost:4100');
export const MERCHANT_URL = env('MERCHANT_URL', 'http://localhost:4000');

export const PORTS = {
  merchant: Number(env('MERCHANT_PORT', 4000)),
  producer: Number(env('PRODUCER_PORT', 4100)),
  consumer: Number(env('CONSUMER_PORT', 4200)),
  healthChecker: Number(env('HEALTH_CHECKER_PORT', 4300)),
  dashboard: Number(env('DASHBOARD_PORT', 8090)),
};

/**
 * Client (merchant) registry. Every client owns a dedicated partition, so a
 * client that is down, slow or rate limiting only ever blocks its own lane.
 * `rateLimitPerSec` is the contracted limit used by the token-bucket strategy.
 */
export const CLIENTS = [
  { id: 'amazon', name: 'Amazon', partition: 0, rateLimitPerSec: 10 },
  { id: 'flipkart', name: 'Flipkart', partition: 1, rateLimitPerSec: 10 },
  { id: 'blinkit', name: 'Blinkit', partition: 2, rateLimitPerSec: 2 },
];

export const PARTITIONS = CLIENTS.length;

export const getClient = (id) => CLIENTS.find((client) => client.id === id);

export const webhookUrl = (clientId) => `${MERCHANT_URL}/merchants/${clientId}/webhook`;
export const healthUrl = (clientId) => `${MERCHANT_URL}/merchants/${clientId}/health`;

/** Delivery settings. Stored in Redis (config:delivery) and editable from the UI. */
export const DEFAULT_SETTINGS = {
  maxAttempts: 5,
  baseBackoffMs: 500,
  maxBackoffMs: 8000,
  requestTimeoutMs: 2000,
  rateLimitStrategy: 'reactive', // reactive | token-bucket | adaptive
  defaultRetryAfterMs: 1000,
  adaptiveStartRate: 8,
  healthCheckIntervalMs: 2000,
  healthyThreshold: 2,
};

export const RATE_LIMIT_STRATEGIES = ['reactive', 'token-bucket', 'adaptive'];
