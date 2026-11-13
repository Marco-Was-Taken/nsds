package it.polimi.middleware.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

// Group number:
// Group members:

// Is it possible to have more than one partition for topics "sensors1" and "sensors2"?
// Yes, because messages with the same key are deterministically routed to the same partition number.

// Is there any relation between the number of partitions in "sensors1" and "sensors2"?
// Yes, they must have the same number of partitions, so that each key maps to the same partition index
// in both topics and each Merger instance receives the matching partitions from both topics.

// Is it possible to have more than one instance of Merger?
// Yes (up to the number of partitions of sensors1/sensors2), provided each instance uses a single
// consumer subscribed to both topics: partition assignment is deterministic and identical across topics
// with the same number of partitions, so one instance handles all messages for a given key from both topics.

// If so, what is the relation between their group id?
// All Merger instances must share the same group.id so that Kafka splits the partitions across them.

// Is it possible to have more than one partition for topic "merged"?
// Yes. Validator is stateless and per-key ordering is preserved within each partition, so multiple
// partitions enable parallel processing by multiple Validator instances.

// Is it possible to have more than one instance of Validator?
// Yes (up to the number of partitions of "merged"), with each instance configured with a unique transactional.id.

// If so, what is the relation between their group id?
// All Validator instances must share the same group.id so each partition (and message) is processed by one instance.

public class ConsumersXX {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: ConsumersXX <stage: 0=Merger, 1=Validator> <groupId> [transactionalId]");
            return;
        }

        String serverAddr = "localhost:9092";
        int stage = Integer.parseInt(args[0]);
        String groupId = args[1];
        // Optional third argument to assign a unique transactional.id when running multiple Validator instances
        String transactionalId = args.length >= 3 ? args[2] : "transactionalValidator";

        switch (stage) {
            case 0:
                new Merger(serverAddr, groupId).execute();
                break;
            case 1:
                new Validator(serverAddr, groupId, transactionalId).execute();
                break;
            default:
                System.err.println("Wrong stage: expected 0 (Merger) or 1 (Validator)");
        }
    }

    private static class Merger {
        private final String serverAddr;
        private final String consumerGroupId;

        private final String inputTopic1 = "sensors1";
        private final String inputTopic2 = "sensors2";
        private final String outputTopic = "merged";

        public Merger(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer configuration: manual commit and read_committed for reliable at-least-once processing
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

            // Use a single consumer subscribed to both topics so partition assignment matches across sensors1 and sensors2
            final KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);

            // Persistent in-memory state for the last seen value of each key in sensors1 and sensors2
            final Map<String, Integer> lastValues1 = new HashMap<>();
            final Map<String, Integer> lastValues2 = new HashMap<>();

            consumer.subscribe(Arrays.asList(inputTopic1, inputTopic2), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Offsets are committed synchronously at the end of every poll batch
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // Rebuild state up to the last committed offset without re-emitting duplicate outputs
                    lastValues1.clear();
                    lastValues2.clear();
                    if (!partitions.isEmpty()) {
                        recoverState(consumer, partitions, lastValues1, lastValues2);
                    }
                }
            });

            // Producer configuration (at-least-once: idempotence not strictly required, flush guarantees persistence)
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            try {
                while (true) {
                    final ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMinutes(1));
                    if (records.isEmpty()) {
                        continue;
                    }

                    // Evaluate every input record individually and emit the updated sum for its key
                    for (final ConsumerRecord<String, Integer> record : records) {
                        final String key = record.key();
                        if (inputTopic1.equals(record.topic())) {
                            lastValues1.put(key, record.value());
                        } else if (inputTopic2.equals(record.topic())) {
                            lastValues2.put(key, record.value());
                        }

                        final int sum = lastValues1.getOrDefault(key, 0) + lastValues2.getOrDefault(key, 0);
                        producer.send(new ProducerRecord<>(outputTopic, key, sum));
                        System.out.println("[Merger] Topic: " + record.topic() + "\tKey: " + key + "\tSum: " + sum);
                    }

                    // Wait for all output records to be acknowledged before committing input offsets
                    producer.flush();
                    consumer.commitSync();
                }
            } finally {
                producer.close();
                consumer.close();
            }
        }

        /**
         * Replays already-committed input records [0, committedOffset) into the in-memory maps
         * after a restart or rebalance, avoiding duplicate output messages on "merged".
         */
        private void recoverState(
                KafkaConsumer<String, Integer> mainConsumer,
                Collection<TopicPartition> partitions,
                Map<String, Integer> lastValues1,
                Map<String, Integer> lastValues2
        ) {
            final Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    mainConsumer.committed(new HashSet<>(partitions));

            final Map<TopicPartition, Long> targetOffsets = new HashMap<>();
            for (final TopicPartition tp : partitions) {
                final OffsetAndMetadata om = committedOffsets.get(tp);
                if (om != null && om.offset() > 0) {
                    targetOffsets.put(tp, om.offset());
                }
            }
            if (targetOffsets.isEmpty()) {
                return;
            }

            // Dedicated consumer without group.id so replay does not affect consumer group offsets
            final Properties recoveryProps = new Properties();
            recoveryProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            recoveryProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            recoveryProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            recoveryProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

            try (KafkaConsumer<String, Integer> recoveryConsumer = new KafkaConsumer<>(recoveryProps)) {
                recoveryConsumer.assign(targetOffsets.keySet());
                recoveryConsumer.seekToBeginning(targetOffsets.keySet());

                final Set<TopicPartition> remaining = new HashSet<>(targetOffsets.keySet());
                remaining.removeIf(tp -> recoveryConsumer.position(tp) >= targetOffsets.get(tp));

                while (!remaining.isEmpty()) {
                    final ConsumerRecords<String, Integer> records = recoveryConsumer.poll(Duration.ofMillis(500));
                    for (final TopicPartition tp : records.partitions()) {
                        if (!remaining.contains(tp)) {
                            continue;
                        }
                        final long target = targetOffsets.get(tp);
                        for (final ConsumerRecord<String, Integer> record : records.records(tp)) {
                            if (record.offset() >= target) {
                                break;
                            }
                            if (inputTopic1.equals(record.topic())) {
                                lastValues1.put(record.key(), record.value());
                            } else if (inputTopic2.equals(record.topic())) {
                                lastValues2.put(record.key(), record.value());
                            }
                        }
                    }

                    final Iterator<TopicPartition> it = remaining.iterator();
                    while (it.hasNext()) {
                        final TopicPartition tp = it.next();
                        if (recoveryConsumer.position(tp) >= targetOffsets.get(tp)) {
                            it.remove();
                            recoveryConsumer.pause(Collections.singletonList(tp));
                        }
                    }

                    if (records.isEmpty() && !remaining.isEmpty()) {
                        final Map<TopicPartition, Long> endOffsets = recoveryConsumer.endOffsets(remaining);
                        remaining.removeIf(tp -> recoveryConsumer.position(tp) >= Math.min(targetOffsets.get(tp), endOffsets.getOrDefault(tp, 0L)));
                    }
                }
            }
        }
    }

    private static class Validator {
        private final String serverAddr;
        private final String consumerGroupId;
        private final String transactionalId;

        private final String inputTopic = "merged";
        private final String outputTopic1 = "output1";
        private final String outputTopic2 = "output2";

        public Validator(String serverAddr, String consumerGroupId, String transactionalId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
            this.transactionalId = transactionalId;
        }

        public void execute() {
            // Consumer configuration: read_committed and manual offset commit inside transactions
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

            final KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic));

            // Producer configuration: transactional.id requires enable.idempotence = true
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
            producer.initTransactions();

            try {
                while (true) {
                    final ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMinutes(1));
                    if (records.isEmpty()) {
                        continue;
                    }

                    try {
                        // Atomically forward every record to both output1 and output2 and commit input offsets
                        producer.beginTransaction();

                        for (final ConsumerRecord<String, Integer> record : records) {
                            producer.send(new ProducerRecord<>(outputTopic1, record.key(), record.value()));
                            producer.send(new ProducerRecord<>(outputTopic2, record.key(), record.value()));
                            System.out.println("[Validator] Key: " + record.key() + "\tValue: " + record.value());
                        }

                        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                        for (final TopicPartition partition : records.partitions()) {
                            final List<ConsumerRecord<String, Integer>> partitionRecords = records.records(partition);
                            final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                            offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                        }

                        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                        producer.commitTransaction();
                    } catch (ProducerFencedException | OutOfOrderSequenceException e) {
                        System.err.println("Fatal transactional error, shutting down Validator: " + e.getMessage());
                        break;
                    } catch (KafkaException e) {
                        // On recoverable failure, abort the transaction and rewind consumer offsets to retry
                        System.err.println("Transaction aborted, rewinding offsets: " + e.getMessage());
                        producer.abortTransaction();
                        for (final TopicPartition partition : records.partitions()) {
                            if (consumer.assignment().contains(partition)) {
                                consumer.seek(partition, records.records(partition).get(0).offset());
                            }
                        }
                    }
                }
            } finally {
                producer.close();
                consumer.close();
            }
        }
    }
}