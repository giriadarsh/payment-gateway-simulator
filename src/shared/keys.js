// Redis key and channel names used across services.

export const K = {
  /** "UP" | "DOWN". A missing key means UP. */
  clientStatus: (id) => `client:${id}:status`,
  /** Exists (with a TTL) while the client is cooling down after an HTTP 429. */
  clientThrottle: (id) => `client:${id}:throttle`,
  /** De-duplication flag: a "check client status" event is already pending. */
  clientHealthCheck: (id) => `client:${id}:healthcheck`,
  /** Contracted requests/sec, used by the token-bucket strategy. */
  clientRateLimit: (id) => `client:${id}:ratelimit`,
  /** Current requests/sec discovered by the adaptive (AIMD) strategy. */
  clientAdaptiveRate: (id) => `client:${id}:adaptive-rate`,
  /** Token bucket state (tokens, last refill timestamp). */
  clientBucket: (id) => `client:${id}:bucket`,
  clientStats: (id) => `stats:client:${id}`,

  /** Hash holding the notification payload and its delivery state. */
  txn: (id) => `txn:${id}`,
  recentTxns: 'txns:recent',

  settings: 'config:delivery',
  epoch: 'sim:epoch',
  /** Partition holds published by the consumer, for the dashboard. */
  holds: 'consumer:holds',
  /** Active health monitors published by the health checker, for the dashboard. */
  monitors: 'health:monitors',
  /** Stream of "check client status" events consumed by the health checker. */
  healthCheckStream: 'stream:client-health-checks',
};

export const CH = {
  /** Visualisation events emitted by every service. */
  events: 'sim:events',
  /** Client status transitions (UP/DOWN); the consumer resumes partitions on UP. */
  clientStatus: 'client-status-changed',
  /** Simulation control messages (RESET). */
  control: 'sim:control',
};

export const HEALTH_GROUP = 'health-checkers';
