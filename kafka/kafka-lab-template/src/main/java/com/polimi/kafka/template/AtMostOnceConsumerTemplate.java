package com.polimi.kafka.template;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * KAFKA TEMPLATE: At-Most-Once Consumer (0 o 1 volta — Mai duplicati, possibili perdite su crash)
 *
 * Quando usarlo:
 * - Es. AtMostOncePrinter (Exam 2023): ogni messaggio deve essere elaborato/stampato 0 o 1 volta,
 *   mai duplicato, e può essere perso solo in caso di crash.
 *
 * Regola chiave:
 * - ENABLE_AUTO_COMMIT_CONFIG = false
 * - Committa gli offset PRIMA di elaborare/stampare il messaggio!
 *   - Modalità per-record (COMMIT_PER_RECORD = true): committa (offset + 1) di ogni singolo record
 *     subito prima di elaborarlo -> in caso di crash perdi al massimo 1 messaggio.
 *   - Modalità per-batch (COMMIT_PER_RECORD = false): chiama consumer.commitSync() subito dopo poll()
 *     e prima del ciclo for -> più veloce (1 commit per batch), in caso di crash perdi i restanti del batch.
 *
 * Argomenti CLI opzionali:
 *   args[0] = groupId (default: "atMostOnceGroup")
 *   args[1] = topic (default: "inputTopic")
 *   args[2] = offsetResetStrategy ("latest" | "earliest", default: "latest")
 */
public class AtMostOnceConsumerTemplate {
    private static final String DEFAULT_GROUP_ID = "atMostOnceGroup";
    private static final String DEFAULT_TOPIC = "inputTopic";
    private static final String DEFAULT_OFFSET_RESET = "latest";
    private static final boolean COMMIT_PER_RECORD = true; // true = perdita max 1 record; false = commit per batch
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String groupId = args.length >= 1 ? args[0] : DEFAULT_GROUP_ID;
        final String topic = args.length >= 2 ? args[1] : DEFAULT_TOPIC;
        final String offsetResetStrategy = args.length >= 3 ? args[2] : DEFAULT_OFFSET_RESET;

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                if (records.isEmpty()) {
                    continue;
                }

                if (!COMMIT_PER_RECORD) {
                    // Opzione B (per-batch): committa TUTTO il batch PRIMA di elaborare qualsiasi record
                    consumer.commitSync();
                }

                for (final ConsumerRecord<String, String> record : records) {
                    if (COMMIT_PER_RECORD) {
                        // Opzione A (per-record): committa QUESTO singolo record PRIMA di elaborarlo
                        final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        final Map<TopicPartition, OffsetAndMetadata> singleOffset =
                                Collections.singletonMap(tp, new OffsetAndMetadata(record.offset() + 1));
                        consumer.commitSync(singleOffset);
                    }

                    // Elabora/stampa DOPO il commit: se crasha qui, al riavvio NON lo rileggerà (mai duplicati!)
                    System.out.println("At-Most-Once -> Partition: " + record.partition()
                            + "\tOffset: " + record.offset()
                            + "\tKey: " + record.key()
                            + "\tValue: " + record.value());
                }
            }
        }
    }
}
