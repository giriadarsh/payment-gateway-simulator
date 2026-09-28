/**
 * Processes one partition's messages strictly in order (per-client ordering).
 *
 * KafkaJS hands over a batch and the partition stays paused until the worker
 * has drained it. That gives backpressure, and because the eachBatch handler
 * returns immediately, a slow, failing or held client never stalls the fetch
 * loop that serves the other partitions.
 *
 * HOLD leaves the head message unacknowledged (neither resolved nor committed)
 * and waits: on a timer (backoff, Retry-After, pacing) or until release() is
 * called (client marked UP, simulation reset).
 */
export class PartitionWorker {
  constructor({ partition, log, processMessage, commit, onDrained, onHold, onRelease }) {
    Object.assign(this, { partition, log, processMessage, commit, onDrained, onHold, onRelease });
    this.queue = [];
    this.running = false;
    this.stopped = false;
    this.hold = null;
  }

  enqueue(messages, resolveOffset) {
    for (const message of messages) this.queue.push({ message, resolveOffset, evaluations: 0 });
    if (!this.running) {
      this.run().catch((err) => this.log.error(`worker P${this.partition} crashed`, { error: err.message }));
    }
  }

  async run() {
    this.running = true;
    try {
      while (!this.stopped && this.queue.length) {
        const item = this.queue[0];
        item.evaluations += 1;

        let decision;
        try {
          decision = await this.processMessage({ partition: this.partition, message: item.message, evaluation: item.evaluations });
        } catch (err) {
          // Infrastructure problem (e.g. Redis unavailable): keep the message, retry shortly.
          this.log.error(`P${this.partition} offset ${item.message.offset} failed`, { error: err.message });
          decision = { action: 'HOLD', reason: 'ERROR', resumeInMs: 1000, quiet: true };
        }
        if (this.stopped) break;

        if (decision.action === 'ACK') {
          item.resolveOffset(item.message.offset);
          try {
            await this.commit(this.partition, item.message.offset, decision);
          } catch (err) {
            // Redelivery after a failed commit is safe: the consumer is idempotent.
            this.log.warn(`commit failed on P${this.partition}`, { error: err.message });
          }
          this.queue.shift();
        } else {
          await this.waitWhileHeld(decision);
        }
      }
    } finally {
      this.running = false;
    }
    if (!this.stopped) this.onDrained(this.partition);
  }

  waitWhileHeld(decision) {
    return new Promise((resolve) => {
      const now = Date.now();
      const hold = {
        ...decision,
        partition: this.partition,
        since: now,
        until: decision.resumeInMs != null ? now + decision.resumeInMs : null,
        resolve,
      };
      if (decision.resumeInMs != null) hold.timer = setTimeout(() => this.release('timer'), decision.resumeInMs);
      this.hold = hold;
      this.onHold(hold);
    });
  }

  /** Ends the current hold; the head message is evaluated again. */
  release(trigger) {
    const hold = this.hold;
    if (!hold) return false;
    this.hold = null;
    clearTimeout(hold.timer);
    this.onRelease(hold, trigger);
    hold.resolve();
    return true;
  }

  /** Drops the worker (partition revoked or consumer restarting). Nothing is committed. */
  stop() {
    this.stopped = true;
    this.queue = [];
    this.release('stopped');
  }
}
