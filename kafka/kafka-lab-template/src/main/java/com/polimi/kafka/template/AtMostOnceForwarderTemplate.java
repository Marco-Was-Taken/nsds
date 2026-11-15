package com.polimi.kafka.template;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * KAFKA TEMPLATE: At-Most-Once Forwarder (0 o 1 volta — Mai duplicati su outputTopic, possibili perdite su crash)
 *
 * Meccanismo:
 * 1. Consumer: ENABLE_AUTO_COMMIT_CONFIG = false; committa l'offset PRIMA di elaborare/inviare il record.
 *    Così in caso di crash il record non verrà mai riletto né reinviato.
 * 2. Producer: ENABLE_IDEMPOTENCE_CONFIG = false, RETRIES_CONFIG = "0", ACKS_CONFIG = "1".
 *    Nessun retry in caso di timeout/errore: se il broker fallisce o l'ACK si perde, non ritrasmette
 *    evitando qualsiasi duplicato in scrittura (possibile perdita al più in caso di failure).
 *
 * Argomenti CLI opzionali:
 *   args[0] = groupId (default: "atMostOnceForwarderGroup")
 *   args[1] = inputTopic (default: "inputTopic")
 *   args[2] = outputTopic (default: "outputTopic")
 */
public class AtMostOnceForwarderTemplate {
    private static final String DEFAULT_GROUP_ID = "atMostOnceForwarderGroup";
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";
    private static final boolean COMMIT_PER_RECORD = true;
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String groupId = args.length >= 1 ? args[0] : DEFAULT_GROUP_ID;
        final String inputTopic = args.length >= 2 ? args[1] : DEFAULT_INPUT_TOPIC;
        final String outputTopic = args.length >= 3 ? args[2] : DEFAULT_OUTPUT_TOPIC;

        // 1. Consumer: commit manuale PRIMA di elaborare/inviare
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 2. Producer: niente retry (retries = 0) per evitare duplicati da ritrasmissione
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(false));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, "0");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Collections.singletonList(inputTopic));

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                if (records.isEmpty()) {
                    continue;
                }

                if (!COMMIT_PER_RECORD) {
                    consumer.commitSync();
                }

                for (final ConsumerRecord<String, String> record : records) {
                    if (COMMIT_PER_RECORD) {
                        final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        final Map<TopicPartition, OffsetAndMetadata> singleOffset =
                                Collections.singletonMap(tp, new OffsetAndMetadata(record.offset() + 1));
                        consumer.commitSync(singleOffset);
                    }

                    final String transformed = record.value().toLowerCase();
                    producer.send(new ProducerRecord<>(outputTopic, record.key(), transformed));
                    System.out.println("[AtMostOnceForwarder] Forwarded <" + record.key() + ", " + transformed + ">");
                }

                // Flush opzionale per inviare subito senza accumulare nel buffer (senza retry comunque)
                producer.flush();
            }
        }
    }
}
