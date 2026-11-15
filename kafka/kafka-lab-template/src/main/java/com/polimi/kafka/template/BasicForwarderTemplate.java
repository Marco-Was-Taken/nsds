package com.polimi.kafka.template;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

/**
 * KAFKA TEMPLATE: Basic Forwarder — Garanzia NONE (Best-Effort / Auto-Commit + Async Send)
 *
 * Quando usarlo:
 * - Quando la traccia specifica esplicitamente "nessuna garanzia in caso di failure"
 *   (es. Lab 1 Ex 1/Ex 2 UpperCaseConsumer, Exam 2024 Consumer2).
 *
 * Comportamento:
 * - Consumer con ENABLE_AUTO_COMMIT_CONFIG = true: possibili sia duplicati (crash prima dell'auto-commit)
 *   sia perdite (auto-commit scattato prima della fine del ciclo for / crash prima che il producer svuoti il buffer).
 * - Producer asincrono senza flush() né transazioni.
 * - Nota: per scalare con più istanze, usa lo STESSO groupId e numPartitions >= numIstanze su inputTopic.
 *
 * Argomenti CLI opzionali:
 *   args[0] = groupId (default: "basicForwarderGroup")
 *   args[1] = inputTopic (default: "inputTopic")
 *   args[2] = outputTopic (default: "outputTopic")
 */
public class BasicForwarderTemplate {
    private static final String DEFAULT_GROUP_ID = "basicForwarderGroup";
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String groupId = args.length >= 1 ? args[0] : DEFAULT_GROUP_ID;
        final String inputTopic = args.length >= 2 ? args[1] : DEFAULT_INPUT_TOPIC;
        final String outputTopic = args.length >= 3 ? args[2] : DEFAULT_OUTPUT_TOPIC;

        // 1. Consumer base con auto-commit attivo (nessuna garanzia)
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(true));
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 2. Producer base fire-and-forget (disabilita idempotenza esplicitamente in Kafka 3.5.1 se vuoi pure None)
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(false));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "0");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Collections.singletonList(inputTopic));

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, String> record : records) {
                    final String transformed = record.value().toLowerCase();
                    producer.send(new ProducerRecord<>(outputTopic, record.key(), transformed));
                    System.out.println("[NoneForwarder] " + record.key() + " -> " + transformed);
                }
            }
        }
    }
}
