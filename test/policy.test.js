import assert from 'node:assert/strict';
import { test } from 'node:test';
import { backoffDelay, classifyResult, nextAdaptiveRate, parseRetryAfter } from '../src/consumer/policy.js';
import { ClientPartitioner } from '../src/producer/partitioner.js';

test('classifyResult maps responses onto delivery decisions', () => {
  assert.equal(classifyResult({ statusCode: 200 }).kind, 'SUCCESS');
  assert.equal(classifyResult({ statusCode: 204 }).kind, 'SUCCESS');
  assert.equal(classifyResult({ statusCode: 503 }).kind, 'RETRYABLE');
  assert.equal(classifyResult({ statusCode: 500 }).kind, 'RETRYABLE');
  assert.equal(classifyResult({ statusCode: 408 }).kind, 'RETRYABLE');
  assert.equal(classifyResult({ error: 'TIMEOUT' }).kind, 'RETRYABLE');
  assert.equal(classifyResult({ statusCode: 400 }).kind, 'PERMANENT');
  assert.equal(classifyResult({ statusCode: 404 }).kind, 'PERMANENT');

  const limited = classifyResult({ statusCode: 429, retryAfter: '2' });
  assert.equal(limited.kind, 'RATE_LIMITED');
  assert.equal(limited.retryAfterMs, 2000);
});

test('parseRetryAfter accepts delay-seconds and HTTP dates', () => {
  const now = Date.parse('2026-01-01T00:00:00Z');
  assert.equal(parseRetryAfter('1', now), 1000);
  assert.equal(parseRetryAfter('0.5', now), 500);
  assert.equal(parseRetryAfter('Thu, 01 Jan 2026 00:00:03 GMT', now), 3000);
  assert.equal(parseRetryAfter('Wed, 31 Dec 2025 23:59:00 GMT', now), 0);
  assert.equal(parseRetryAfter(null, now), null);
  assert.equal(parseRetryAfter('soon', now), null);
});

test('backoffDelay doubles per attempt, is capped and jittered by ±10%', () => {
  const settings = { baseBackoffMs: 500, maxBackoffMs: 3000 };
  const noJitter = () => 0.5;
  assert.deepEqual(
    [1, 2, 3, 4, 5].map((attempt) => backoffDelay(attempt, settings, noJitter)),
    [500, 1000, 2000, 3000, 3000],
  );
  assert.equal(backoffDelay(2, settings, () => 0), 900);
  assert.equal(backoffDelay(2, settings, () => 1), 1100);
});

test('nextAdaptiveRate halves on 429 and creeps up on success', () => {
  assert.equal(nextAdaptiveRate(8, 'throttled'), 4);
  assert.equal(nextAdaptiveRate(4, 'ok'), 4.25);
  assert.equal(nextAdaptiveRate(0.6, 'throttled'), 0.5);
  assert.equal(nextAdaptiveRate(50, 'ok'), 50);
});

test('ClientPartitioner pins each registered client to its own partition', () => {
  const partitioner = ClientPartitioner();
  const partitionMetadata = [0, 1, 2].map((partitionId) => ({ partitionId, leader: 1 }));
  const partitionOf = (key) => partitioner({ topic: 't', partitionMetadata, message: { key: Buffer.from(key) } });
  assert.deepEqual(['amazon', 'flipkart', 'blinkit'].map(partitionOf), [0, 1, 2]);
  // Unknown clients fall back to hashing, deterministically.
  assert.equal(partitionOf('someone-else'), partitionOf('someone-else'));
});
