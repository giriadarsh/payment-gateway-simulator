import assert from 'node:assert/strict';
import { test } from 'node:test';
import { PartitionWorker } from '../src/consumer/partition-worker.js';

const silentLog = { info() {}, warn() {}, error() {} };
const messages = (...offsets) => offsets.map((offset) => ({ offset: String(offset), value: Buffer.from('{}') }));
const tick = () => new Promise((resolve) => setImmediate(resolve));

function createWorker(decide) {
  const calls = { processed: [], resolved: [], committed: [], drained: 0, holds: [] };
  const worker = new PartitionWorker({
    partition: 0,
    log: silentLog,
    processMessage: async ({ message, evaluation }) => {
      calls.processed.push(`${message.offset}#${evaluation}`);
      return decide(message, evaluation);
    },
    commit: async (_partition, offset) => calls.committed.push(offset),
    onDrained: () => {
      calls.drained += 1;
    },
    onHold: (hold) => calls.holds.push(hold.reason),
    onRelease: () => {},
  });
  const resolveOffset = (offset) => calls.resolved.push(offset);
  return { worker, calls, resolveOffset };
}

test('acknowledges messages in order and resumes the partition when drained', async () => {
  const { worker, calls, resolveOffset } = createWorker(() => ({ action: 'ACK' }));
  worker.enqueue(messages(5, 6, 7), resolveOffset);
  await tick();
  assert.deepEqual(calls.committed, ['5', '6', '7']);
  assert.deepEqual(calls.resolved, ['5', '6', '7']);
  assert.equal(calls.drained, 1);
});

test('a held message stays unacknowledged and blocks the messages behind it', async () => {
  let clientUp = false;
  const { worker, calls, resolveOffset } = createWorker(() => (clientUp ? { action: 'ACK' } : { action: 'HOLD', reason: 'CLIENT_DOWN' }));
  worker.enqueue(messages(1, 2), resolveOffset);
  await tick();

  assert.deepEqual(calls.holds, ['CLIENT_DOWN']);
  assert.deepEqual(calls.committed, [], 'nothing is committed while the client is down');
  assert.equal(calls.drained, 0, 'partition stays paused');
  assert.equal(worker.hold.reason, 'CLIENT_DOWN');

  clientUp = true;
  assert.equal(worker.release('status-up'), true);
  await tick();
  assert.deepEqual(calls.processed, ['1#1', '1#2', '2#1'], 'the held message is re-evaluated first');
  assert.deepEqual(calls.committed, ['1', '2']);
  assert.equal(calls.drained, 1);
});

test('timed holds (backoff, Retry-After) release themselves', async () => {
  const { worker, calls, resolveOffset } = createWorker((_message, evaluation) =>
    evaluation === 1 ? { action: 'HOLD', reason: 'BACKOFF', resumeInMs: 20 } : { action: 'ACK' },
  );
  worker.enqueue(messages(9), resolveOffset);
  await tick();
  assert.deepEqual(calls.committed, []);
  await new Promise((resolve) => setTimeout(resolve, 40));
  assert.deepEqual(calls.committed, ['9']);
});

test('stop() drops queued messages without committing them', async () => {
  const { worker, calls, resolveOffset } = createWorker(() => ({ action: 'HOLD', reason: 'CLIENT_DOWN' }));
  worker.enqueue(messages(1, 2, 3), resolveOffset);
  await tick();
  worker.stop();
  await tick();
  assert.deepEqual(calls.committed, []);
  assert.equal(calls.drained, 0);
  assert.equal(worker.queue.length, 0);
});
