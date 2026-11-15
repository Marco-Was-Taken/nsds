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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * KAFKA TEMPLATE: At-Least-Once Forwarder (Stateless o Stateful con Replay — Rif. Lab 2 Merger)
 *
 * Quando usarlo:
 * - Ogni messaggio di input deve essere elaborato e inoltrato ALMENO una volta (mai perso).
 * - In caso di crash tra producer.flush() e consumer.commitSync(), al riavvio i record non ancora
 *   committati vengono rielaborati e re-inviati (possibili duplicati).
 *
 * Regola d'oro (ordine obbligatorio):
 *   1. Elabora e chiama producer.send(...)
 *   2. Chiama producer.flush() (attende l'ACK del broker per TUTTI i record inviati nel batch!)
 *   3. Solo DOPO chiama consumer.commitSync() (mai prima del flush, altrimenti rischieresti perdite!)
 *
 * Modalità Stateful (STATEFUL_RECOVERY = true, stile Lab 2 Merger):
 * - In caso di crash/rebalance, un ConsumerRebalanceListener ricostruisce lo stato in RAM leggendo
 *   i record già committati [0, committedOffset) tramite un consumer dedicato (senza group.id),
 *   SENZA ri-emettere messaggi verso outputTopic, minimizzando i duplicati!
 *
 * Argomenti CLI opzionali:
 *   args[0] = groupId (default: "atLeastOnceForwarderGroup")
 *   args[1] = inputTopic (default: "inputTopic")
 *   args[2] = outputTopic (default: "outputTopic")
 */
public class AtLeastOnceForwarderTemplate {
    private static final String DEFAULT_GROUP_ID = "atLeastOnceForwarderGroup";
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";
    private static final boolean STATEFUL_RECOVERY = true; // true = ricostruisce stato fino a committed() su rebalance
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String groupId = args.length >= 1 ? args[0] : DEFAULT_GROUP_ID;
        final String inputTopic = args.length >= 2 ? args[1] : DEFAULT_INPUT_TOPIC;
        final String outputTopic = args.length >= 3 ? args[2] : DEFAULT_OUTPUT_TOPIC;

        // 1. Consumer: manual commit + read_committed
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 2. Producer: acks="all" + retries abilitati (idempotenza opzionale: false = At-Least-Once puro, true = no retry duplicates)
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(false));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));

        final Map<String, String> lastValueByKey = new HashMap<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {

            if (STATEFUL_RECOVERY) {
                consumer.subscribe(Collections.singletonList(inputTopic), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        // Gli offset vengono committati sincroni a fine di ogni batch
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        lastValueByKey.clear();
                        if (!partitions.isEmpty()) {
                            recoverStateUpToCommitted(consumer, partitions, lastValueByKey);
                        }
                    }
                });
            } else {
                consumer.subscribe(Collections.singletonList(inputTopic));
            }

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(1));
                if (records.isEmpty()) {
                    continue;
                }

                for (final ConsumerRecord<String, String> record : records) {
                    lastValueByKey.put(record.key(), record.value());
                    final String outValue = record.value().toLowerCase();
                    producer.send(new ProducerRecord<>(outputTopic, record.key(), outValue));
                    System.out.println("[AtLeastOnceForwarder] Key: " + record.key() + "\tOut: " + outValue);
                }

                // ORDINE CRITICO: prima attendi persistenza su outputTopic (flush), poi committa inputTopic (commitSync)!
                producer.flush();
                consumer.commitSync();
            }
        }
    }

    /**
     * Rilegge i soli record già committati [0, committedOffset) per ripristinare lo stato in RAM
     * senza ri-emettere messaggi in output (pattern Lab 2 Merger).
     */
    private static void recoverStateUpToCommitted(
            KafkaConsumer<String, String> mainConsumer,
            Collection<TopicPartition> partitions,
            Map<String, String> stateMap
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

        // Consumer di recovery SENZA group.id per non alterare gli offset del Consumer Group principale
        final Properties recoveryProps = new Properties();
        recoveryProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        recoveryProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        recoveryProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        recoveryProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        try (KafkaConsumer<String, String> recoveryConsumer = new KafkaConsumer<>(recoveryProps)) {
            recoveryConsumer.assign(targetOffsets.keySet());
            recoveryConsumer.seekToBeginning(targetOffsets.keySet());

            final Set<TopicPartition> remaining = new HashSet<>(targetOffsets.keySet());
            remaining.removeIf(tp -> recoveryConsumer.position(tp) >= targetOffsets.get(tp));

            while (!remaining.isEmpty()) {
                final ConsumerRecords<String, String> records = recoveryConsumer.poll(Duration.ofMillis(500));
                for (final TopicPartition tp : records.partitions()) {
                    if (!remaining.contains(tp)) {
                        continue;
                    }
                    final long target = targetOffsets.get(tp);
                    for (final ConsumerRecord<String, String> record : records.records(tp)) {
                        if (record.offset() >= target) {
                            break;
                        }
                        stateMap.put(record.key(), record.value());
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
                    remaining.removeIf(tp -> recoveryConsumer.position(tp)
                            >= Math.min(targetOffsets.get(tp), endOffsets.getOrDefault(tp, 0L)));
                }
            }
        }
    }
}
