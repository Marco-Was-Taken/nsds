package com.polimi.kafka.template;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * KAFKA TEMPLATE 7: Stateless Atomic Forwarder (End-to-End Exactly-Once / EOS)
 *
 * Quando usarlo:
 * - Trasformazione stateless Read-Process-Write: legge da inputTopic, trasforma ogni record,
 *   scrive su outputTopic e committa gli offset del consumer nella STESSA transazione atomica.
 *
 * Regole critiche:
 * 1. Consumer: ENABLE_AUTO_COMMIT_CONFIG = false, ISOLATION_LEVEL_CONFIG = "read_committed".
 *    Il consumer NON chiama mai commitSync(): ci pensa producer.sendOffsetsToTransaction(...).
 * 2. Producer: TRANSACTIONAL_ID_CONFIG impostato + ENABLE_IDEMPOTENCE_CONFIG = true.
 * 3. Scaling multi-istanza: tutte le istanze condividono lo STESSO groupId, ma OGNI istanza deve
 *    ricevere un transactionalId UNIVOCO (es. via args[0]), altrimenti scatta Zombie Fencing!
 * 4. Recovery errori:
 *    - ProducerFencedException / OutOfOrderSequenceException -> errore fatale, chiudere e uscire.
 *    - KafkaException -> abortTransaction() + consumer.seek(partition, firstOffset) per rileggere il batch.
 *
 * Argomenti CLI opzionali:
 *   args[0] = transactionalId (default: "forwarderTx-1")
 *   args[1] = consumerGroupId (default: "forwarderGroup")
 *   args[2] = inputTopic (default: "inputTopic")
 *   args[3] = outputTopic (default: "outputTopic")
 */
public class TransactionalForwarderTemplate {
    private static final String DEFAULT_TRANSACTIONAL_ID = "forwarderTx-1";
    private static final String DEFAULT_CONSUMER_GROUP_ID = "forwarderGroup";
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String transactionalId = args.length >= 1 ? args[0] : DEFAULT_TRANSACTIONAL_ID;
        final String consumerGroupId = args.length >= 2 ? args[1] : DEFAULT_CONSUMER_GROUP_ID;
        final String inputTopic = args.length >= 3 ? args[2] : DEFAULT_INPUT_TOPIC;
        final String outputTopic = args.length >= 4 ? args[3] : DEFAULT_OUTPUT_TOPIC;

        // 1. Consumer configuration (manual commit via producer transaction)
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(inputTopic));

        // 2. Transactional Producer configuration
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));

        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        producer.initTransactions();

        try {
            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                if (records.isEmpty()) {
                    continue;
                }

                try {
                    producer.beginTransaction();

                    for (final ConsumerRecord<String, String> record : records) {
                        // Trasformazione stateless di esempio
                        final String transformedValue = record.value().toLowerCase();
                        producer.send(new ProducerRecord<>(outputTopic, record.key(), transformedValue));
                    }

                    // Committa atomicamente gli offset del consumer (lastOffset + 1 per ogni partizione letta)
                    final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (final TopicPartition partition : records.partitions()) {
                        final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                        final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }

                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                    producer.commitTransaction();
                } catch (ProducerFencedException | OutOfOrderSequenceException e) {
                    System.err.println("Fatal fencing/sequence error, shutting down: " + e.getMessage());
                    break;
                } catch (KafkaException e) {
                    System.err.println("Transaction aborted, seeking back to batch start: " + e.getMessage());
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
