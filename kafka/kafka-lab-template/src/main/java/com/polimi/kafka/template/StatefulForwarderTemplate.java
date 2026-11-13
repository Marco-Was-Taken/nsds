package com.polimi.kafka.template;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * KAFKA TEMPLATE 8: Stateful Fault-Tolerant EOS Forwarder (Lab 1 Es. 4 + Multi-Instance Rebalance Safe)
 *
 * Quando usarlo:
 * - Elaborazione STATEFUL con semantica Exactly-Once (es. contatore/aggregazione per key in RAM).
 * - L'outputTopic (idealmente creato con Log Compaction tramite TopicAdminTemplate) funge sia da output
 *   sia da changelog persistente per ripristinare lo stato in RAM dopo crash o rebalance.
 *
 * Meccanismi chiave:
 * 1. State Recovery (assign + seekToBeginning): uno storeConsumer con isolation.level = "read_committed"
 *    legge tutte le partizioni di outputTopic dall'offset 0 e ricostruisce la Map<String, Integer> in RAM.
 * 2. Multi-Instance Rebalance Safety: il ConsumerRebalanceListener richiama recoverStateFromChangelog(...)
 *    in onPartitionsAssigned(...), così se un'istanza eredita una partizione da un'altra istanza caduta,
 *    aggiorna la propria RAM prima di elaborare i nuovi record.
 * 3. Rollback-Safe State Update: le modifiche in RAM sono accumulate in pendingUpdates e applicate a stateMap
 *    SOLO dopo producer.commitTransaction() (se abortisce, stateMap resta consistente col log committato!).
 *
 * Argomenti CLI opzionali:
 *   args[0] = transactionalId (default: "statefulForwarderTx-1") -> UNIVOCO per ogni istanza!
 *   args[1] = consumerGroupId (default: "statefulGroup")
 *   args[2] = inputTopic (default: "inputTopic")
 *   args[3] = outputTopic (default: "outputTopic")
 */
public class StatefulForwarderTemplate {
    private static final String DEFAULT_TRANSACTIONAL_ID = "statefulForwarderTx-1";
    private static final String DEFAULT_CONSUMER_GROUP_ID = "statefulGroup";
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String transactionalId = args.length >= 1 ? args[0] : DEFAULT_TRANSACTIONAL_ID;
        final String consumerGroupId = args.length >= 2 ? args[1] : DEFAULT_CONSUMER_GROUP_ID;
        final String inputTopic = args.length >= 3 ? args[2] : DEFAULT_INPUT_TOPIC;
        final String outputTopic = args.length >= 4 ? args[3] : DEFAULT_OUTPUT_TOPIC;

        // 1. Configurazione Consumer principale
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2. Configurazione Store Consumer per ricostruire lo stato da outputTopic (senza group.id!)
        final Properties storeProps = new Properties();
        storeProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        storeProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        storeProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        storeProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        final Map<String, Integer> countMap = new HashMap<>();

        // Recovery iniziale all'avvio
        recoverStateFromChangelog(storeProps, outputTopic, countMap);

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(inputTopic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // Nessun offset non committato in sospeso: le transazioni committano dentro il loop
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // Multi-istanza: se cambiano le partizioni assegnate, ricarica lo stato più recente dal changelog
                if (!partitions.isEmpty()) {
                    recoverStateFromChangelog(storeProps, outputTopic, countMap);
                }
            }
        });

        // 3. Producer transazionale (chiave String, valore Integer = stato aggiornato)
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));

        final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
        producer.initTransactions();

        try {
            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                if (records.isEmpty()) {
                    continue;
                }

                // Buffer locale per aggiornare countMap SOLO a transazione committata
                final Map<String, Integer> pendingUpdates = new HashMap<>();

                try {
                    producer.beginTransaction();

                    for (final ConsumerRecord<String, String> record : records) {
                        final int current = pendingUpdates.containsKey(record.key())
                                ? pendingUpdates.get(record.key())
                                : countMap.getOrDefault(record.key(), 0);
                        final int newCount = current + 1;
                        pendingUpdates.put(record.key(), newCount);

                        producer.send(new ProducerRecord<>(outputTopic, record.key(), newCount));
                        System.out.println("Key: " + record.key() + "\tNewCount: " + newCount);
                    }

                    final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (final TopicPartition partition : records.partitions()) {
                        final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                        final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }

                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                    producer.commitTransaction();

                    // Commit riuscito: applica gli aggiornamenti di stato in RAM
                    countMap.putAll(pendingUpdates);
                } catch (ProducerFencedException | OutOfOrderSequenceException e) {
                    System.err.println("Fatal fencing/sequence error, shutting down: " + e.getMessage());
                    break;
                } catch (KafkaException e) {
                    System.err.println("Transaction aborted, rolling back and seeking: " + e.getMessage());
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

    /**
     * Ricostruisce la mappa di stato rileggendo tutte le partizioni del topic compattato dall'inizio.
     */
    private static void recoverStateFromChangelog(Properties storeProps, String changelogTopic, Map<String, Integer> stateMap) {
        stateMap.clear();
        try (KafkaConsumer<String, Integer> storeConsumer = new KafkaConsumer<>(storeProps)) {
            final List<PartitionInfo> infos = storeConsumer.partitionsFor(changelogTopic);
            if (infos == null || infos.isEmpty()) {
                return;
            }
            final List<TopicPartition> partitions = infos.stream()
                    .map(p -> new TopicPartition(changelogTopic, p.partition()))
                    .toList();
            storeConsumer.assign(partitions);
            storeConsumer.seekToBeginning(partitions);

            while (true) {
                final ConsumerRecords<String, Integer> stored = storeConsumer.poll(Duration.ofSeconds(1));
                if (stored.isEmpty()) {
                    break;
                }
                for (final ConsumerRecord<String, Integer> record : stored) {
                    stateMap.put(record.key(), record.value());
                }
            }
            System.out.println("Recovered state from " + changelogTopic + ": " + stateMap.size() + " keys loaded.");
        }
    }
}
