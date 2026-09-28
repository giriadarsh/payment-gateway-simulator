import { Partitioners } from 'kafkajs';
import { getClient } from '../shared/config.js';

/**
 * Client-specific partitioner. The message key is the client id; a registered
 * client always lands on its own dedicated partition, so one client's backlog
 * (outage, retries, rate limiting) can never delay another client.
 * Unknown clients fall back to KafkaJS's default murmur2 hashing of the key.
 */
export const ClientPartitioner = () => {
  const hashPartitioner = Partitioners.DefaultPartitioner();
  return (args) => {
    const client = getClient(args.message.key?.toString());
    return client ? client.partition % args.partitionMetadata.length : hashPartitioner(args);
  };
};
