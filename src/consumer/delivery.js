// Decides what happens to one notification message. The result is either
// ACK (commit the offset) or HOLD (leave the message unacknowledged; the
// partition worker waits, keeping the client's partition paused).
import { getClient, webhookUrl } from '../shared/config.js';
import { describeFetchError } from '../shared/http.js';
import { CH, K } from '../shared/keys.js';
import { formatMs } from '../shared/util.js';
import { backoffDelay, classifyResult } from './policy.js';

/** TTL of the "health check pending" flag; the health checker keeps refreshing it. */
const HEALTH_FLAG_TTL_MS = 30_000;

const ack = (base, extra = {}) => ({ action: 'ACK', ...base, ...extra });
const hold = (reason, base, extra = {}) => ({ action: 'HOLD', reason, ...base, ...extra });

export function createDeliveryProcessor({ redis, emit, getSettings, limiter }) {
  const stat = (clientId, field) => redis.hincrby(K.clientStats(clientId), field, 1);

  /**
   * Adds a "check client status" event to the Redis stream, once per outage:
   * the NX flag de-duplicates requests from every held message and consumer.
   */
  async function requestHealthCheck(clientId, reason, base = { clientId }) {
    const flag = await redis.set(
      K.clientHealthCheck(clientId),
      JSON.stringify({ reason, requestedAt: Date.now() }),
      'PX',
      HEALTH_FLAG_TTL_MS,
      'NX',
    );
    if (flag !== 'OK') return false;
    const streamId = await redis.xadd(
      K.healthCheckStream, 'MAXLEN', '~', 1000, '*',
      'clientId', clientId, 'reason', reason, 'requestedAt', String(Date.now()),
    );
    emit('HEALTH_CHECK_REQUESTED', {
      ...base,
      streamId,
      reason,
      msg: `XADD ${K.healthCheckStream} {clientId: ${clientId}} — asks the health checker to watch the client`,
    });
    return true;
  }

  async function markClientDown(clientId, base, attempts) {
    await redis.set(K.clientStatus(clientId), 'DOWN');
    await redis.publish(CH.clientStatus, JSON.stringify({ clientId, status: 'DOWN', by: 'consumer', ts: Date.now() }));
    await stat(clientId, 'markedDown');
    emit('CLIENT_MARKED_DOWN', {
      ...base,
      attempts,
      msg: `Retry budget exhausted after ${attempts} attempts → SET ${K.clientStatus(clientId)} DOWN`,
    });
  }

  async function postWebhook(clientId, payload, { eventId, attempt, timeoutMs }) {
    const started = Date.now();
    try {
      const res = await fetch(webhookUrl(clientId), {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'idempotency-key': eventId,
          'x-notification-attempt': String(attempt),
        },
        body: payload,
        signal: AbortSignal.timeout(timeoutMs),
      });
      await res.arrayBuffer().catch(() => {});
      return { statusCode: res.status, retryAfter: res.headers.get('retry-after'), latencyMs: Date.now() - started };
    } catch (err) {
      return { error: describeFetchError(err), latencyMs: Date.now() - started };
    }
  }

  async function processMessage({ partition, message, evaluation }) {
    const offset = Number(message.offset);
    let event;
    try {
      event = JSON.parse(message.value.toString());
    } catch {
      emit('DEAD_LETTERED', { partition, offset, msg: `Unparseable message at offset ${offset} skipped` });
      return ack({ partition, offset });
    }

    const { txnId, clientId } = event;
    const base = { txnId, clientId, partition, offset };

    // Messages from before the last simulation reset are acknowledged silently.
    const epoch = Number(await redis.get(K.epoch)) || 0;
    if ((Number(event.epoch) || 0) < epoch) return ack(base, { stale: true });

    if (evaluation === 1) {
      emit('MESSAGE_CONSUMED', { ...base, msg: `Read ${txnId} from partition ${partition} (offset ${offset})` });
    }

    const settings = await getSettings();
    const txn = await redis.hgetall(K.txn(txnId));

    // Idempotent consumer: a redelivered message that was already delivered is not sent again.
    if (txn.state === 'DELIVERED') {
      emit('DUPLICATE_SKIPPED', { ...base, msg: `${txnId} was already delivered — acknowledged without calling the client` });
      return ack(base);
    }

    // Gate 1 — only process when the client is not DOWN (missing key = UP).
    const status = (await redis.get(K.clientStatus(clientId))) ?? 'UP';
    emit('STATUS_CHECKED', { ...base, status, level: status === 'UP' ? 'debug' : 'info', msg: `GET ${K.clientStatus(clientId)} → ${status}` });
    if (status === 'DOWN') {
      await redis.hset(K.txn(txnId), 'state', 'HELD_CLIENT_DOWN');
      await requestHealthCheck(clientId, 'status-gate', base);
      return hold('CLIENT_DOWN', base);
    }

    // Gate 2 — cooling down after a 429 (TTL key shared by all consumer instances).
    const coolDownMs = await redis.pttl(K.clientThrottle(clientId));
    if (coolDownMs > 0) {
      await redis.hset(K.txn(txnId), 'state', 'RATE_LIMITED');
      return hold('RATE_LIMITED', base, { resumeInMs: coolDownMs });
    }

    // Gate 3 — a backoff scheduled by an earlier failed attempt (restart-safe).
    const retryInMs = Number(txn.nextAttemptAt || 0) - Date.now();
    if (retryInMs > 0) return hold('BACKOFF', base, { resumeInMs: retryInMs, attempt: Number(txn.attempts) + 1 });

    // Gate 4 — proactive client-side rate limiting (token bucket or adaptive).
    if (settings.rateLimitStrategy !== 'reactive') {
      const rate = await limiter.rateFor(clientId, settings);
      const { allowed, waitMs } = await limiter.take(clientId, rate);
      if (!allowed) {
        await stat(clientId, 'paced');
        emit('THROTTLED', {
          ...base,
          waitMs,
          rate,
          strategy: settings.rateLimitStrategy,
          level: 'debug',
          msg: `Token bucket for ${clientId} is empty at ${rate} req/s → wait ${formatMs(waitMs)} before calling`,
        });
        return hold('PACING', base, { resumeInMs: waitMs, rate, quiet: true });
      }
    }

    if (!txn.payload) {
      await stat(clientId, 'deadLettered');
      emit('DEAD_LETTERED', { ...base, msg: `No payload in Redis for ${txnId} → dead-lettered` });
      return ack(base);
    }
    emit('PAYLOAD_FETCHED', { ...base, level: 'debug', msg: `HGET ${K.txn(txnId)} payload → ${txn.payload.length} bytes` });

    const attempt = Number(txn.attempts || 0) + 1;
    await redis.hset(K.txn(txnId), { state: 'DELIVERING', attempts: attempt, lastAttemptAt: Date.now() });
    await redis.hincrby(K.txn(txnId), 'totalAttempts', 1);
    await stat(clientId, 'attempts');
    emit('DELIVERY_ATTEMPT', {
      ...base,
      attempt,
      maxAttempts: settings.maxAttempts,
      msg: `POST ${getClient(clientId)?.name ?? clientId} webhook — attempt ${attempt}/${settings.maxAttempts}`,
    });

    const result = await postWebhook(clientId, txn.payload, {
      eventId: event.eventId ?? txn.eventId,
      attempt,
      timeoutMs: settings.requestTimeoutMs,
    });
    const outcome = classifyResult(result);
    const detail = { ...base, attempt, statusCode: result.statusCode, error: result.error, latencyMs: result.latencyMs, result: outcome.reason };

    switch (outcome.kind) {
      case 'SUCCESS': {
        await redis.hset(K.txn(txnId), { state: 'DELIVERED', deliveredAt: Date.now(), lastResult: outcome.reason, nextAttemptAt: 0 });
        await stat(clientId, 'delivered');
        await limiter.adapt(clientId, 'ok', settings);
        emit('DELIVERY_SUCCEEDED', { ...detail, msg: `${clientId} answered ${outcome.reason} in ${formatMs(result.latencyMs)} — delivered` });
        return ack(base);
      }

      case 'RATE_LIMITED': {
        const retryAfterMs = outcome.retryAfterMs ?? settings.defaultRetryAfterMs;
        await redis.set(K.clientThrottle(clientId), JSON.stringify({ retryAfterMs, at: Date.now() }), 'PX', Math.max(1, retryAfterMs));
        // A 429 means the client is alive: it does not consume the retry budget.
        await redis.hset(K.txn(txnId), { state: 'RATE_LIMITED', attempts: attempt - 1, lastResult: outcome.reason });
        await stat(clientId, 'rateLimited');
        emit('RATE_LIMITED', {
          ...detail,
          retryAfterMs,
          msg: `${clientId} answered 429 → SET ${K.clientThrottle(clientId)} PX ${retryAfterMs}; retry budget untouched`,
        });
        const change = await limiter.adapt(clientId, 'throttled', settings);
        if (change && change.to !== change.from) {
          emit('ADAPTIVE_RATE_CHANGED', { ...base, from: change.from, to: change.to, msg: `Adaptive rate for ${clientId}: ${change.from} → ${change.to} req/s` });
        }
        return hold('RATE_LIMITED', base, { resumeInMs: retryAfterMs });
      }

      case 'PERMANENT': {
        await redis.hset(K.txn(txnId), { state: 'FAILED', lastResult: outcome.reason });
        await stat(clientId, 'deadLettered');
        emit('DEAD_LETTERED', { ...detail, msg: `${clientId} rejected ${txnId} with ${outcome.reason} — not retryable, dead-lettered` });
        return ack(base);
      }

      default: {
        await stat(clientId, 'failures');
        if (attempt < settings.maxAttempts) {
          const delayMs = backoffDelay(attempt, settings);
          await redis.hset(K.txn(txnId), { state: 'RETRY_SCHEDULED', nextAttemptAt: Date.now() + delayMs, lastResult: outcome.reason });
          emit('DELIVERY_FAILED', {
            ...detail,
            delayMs,
            nextAttempt: attempt + 1,
            msg: `Attempt ${attempt} failed (${outcome.reason}) → exponential backoff, retry in ${formatMs(delayMs)}`,
          });
          return hold('BACKOFF', base, { resumeInMs: delayMs, attempt: attempt + 1 });
        }
        // Retry budget exhausted: mark the client DOWN and keep the message
        // unacknowledged. It gets a fresh retry budget once the client is UP.
        await redis.hset(K.txn(txnId), { state: 'HELD_CLIENT_DOWN', attempts: 0, nextAttemptAt: 0, lastResult: outcome.reason });
        emit('DELIVERY_FAILED', { ...detail, exhausted: true, msg: `Attempt ${attempt}/${settings.maxAttempts} failed (${outcome.reason})` });
        await markClientDown(clientId, base, attempt);
        await requestHealthCheck(clientId, 'retries-exhausted', base);
        return hold('CLIENT_DOWN', base);
      }
    }
  }

  return { processMessage, requestHealthCheck };
}
