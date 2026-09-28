import { Kafka, logLevel } from 'kafkajs';
import { KAFKA_BROKERS, PARTITIONS, TOPIC } from './config.js';

export function createKafka(clientId) {
  return new Kafka({
    clientId,
    brokers: KAFKA_BROKERS,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 30 },
  });
}

/**
 * Idempotently creates the notification topic with one partition per client.
 * Every Kafka-facing service calls this on start-up, so start order does not matter.
 */
export async function ensureTopic(kafka, log) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    if (!(await admin.listTopics()).includes(TOPIC)) {
      // Returns false (no error) if another service created it in the meantime.
      const created = await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: TOPIC, numPartitions: PARTITIONS, replicationFactor: 1 }],
      });
      if (created) log.info(`created topic ${TOPIC} with ${PARTITIONS} partitions`);
    }

    const { topics } = await admin.fetchTopicMetadata({ topics: [TOPIC] });
    const count = topics[0]?.partitions.length ?? 0;
    if (count < PARTITIONS) {
      await admin.createPartitions({ topicPartitions: [{ topic: TOPIC, count: PARTITIONS }] });
      log.info(`increased ${TOPIC} partitions from ${count} to ${PARTITIONS}`);
    }
  } finally {
    await admin.disconnect();
  }
}
