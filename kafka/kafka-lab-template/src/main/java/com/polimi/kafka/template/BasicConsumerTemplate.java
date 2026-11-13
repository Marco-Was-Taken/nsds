package com.polimi.kafka.template;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

/**
 * KAFKA TEMPLATE 4: Basic Consumer (Auto-Commit)
 *
 * Concetti chiave:
 * - groupId UGUALE su più istanze -> Load Balancing (ogni partizione va a 1 solo consumer del gruppo).
 * - groupId DIVERSO su più istanze -> Broadcast / Pub-Sub (ogni gruppo riceve tutti i messaggi).
 * - auto.offset.reset ("latest" | "earliest"): usato SOLO se per quel groupId non esistono già offset salvati sul broker!
 * - Con enable.auto.commit = true non ci sono garanzie forti (possibili duplicati o perdite in caso di crash).
 *
 * Argomenti CLI opzionali:
 *   args[0] = groupId (default: "groupA")
 *   args[1] = topic (default: "inputTopic")
 *   args[2] = offsetResetStrategy ("latest" | "earliest", default: "latest")
 */
public class BasicConsumerTemplate {
    private static final String DEFAULT_GROUP_ID = "groupA";
    private static final String DEFAULT_TOPIC = "inputTopic";
    private static final String DEFAULT_OFFSET_RESET = "latest";
    private static final int AUTO_COMMIT_INTERVAL_MS = 15000;
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String groupId = args.length >= 1 ? args[0] : DEFAULT_GROUP_ID;
        final String topic = args.length >= 2 ? args[1] : DEFAULT_TOPIC;
        final String offsetResetStrategy = args.length >= 3 ? args[2] : DEFAULT_OFFSET_RESET;

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(true));
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(AUTO_COMMIT_INTERVAL_MS));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, String> record : records) {
                    System.out.println("Consumer group: " + groupId
                            + "\tPartition: " + record.partition()
                            + "\tOffset: " + record.offset()
                            + "\tKey: " + record.key()
                            + "\tValue: " + record.value());
                }
            }
        }
    }
}
