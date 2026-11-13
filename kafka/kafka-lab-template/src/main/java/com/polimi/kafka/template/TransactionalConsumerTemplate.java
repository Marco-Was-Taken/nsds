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
 * KAFKA TEMPLATE 6: Transactional Consumer (read_committed vs read_uncommitted)
 *
 * Quando usarlo:
 * - Consumer a valle di un TransactionalProducer o di un Atomic/Stateful Forwarder (EOS).
 * - Con ISOLATION_LEVEL_CONFIG = "read_committed", scarta automaticamente i record appartenenti
 *   a transazioni abortite o non ancora committate.
 * - Attenzione: il default di Kafka è "read_uncommitted"! Va sempre impostato esplicitamente.
 *
 * Argomenti CLI opzionali:
 *   args[0] = groupId (default: "txConsumerGroup")
 *   args[1] = topic (default: "outputTopic")
 *   args[2] = readUncommitted ("true" | "false", default: "false" -> read_committed)
 */
public class TransactionalConsumerTemplate {
    private static final String DEFAULT_GROUP_ID = "txConsumerGroup";
    private static final String DEFAULT_TOPIC = "outputTopic";
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String groupId = args.length >= 1 ? args[0] : DEFAULT_GROUP_ID;
        final String topic = args.length >= 2 ? args[1] : DEFAULT_TOPIC;
        final boolean readUncommitted = args.length >= 3 && Boolean.parseBoolean(args[2]);

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, readUncommitted ? "read_uncommitted" : "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                if (records.isEmpty()) {
                    continue;
                }

                for (final ConsumerRecord<String, String> record : records) {
                    System.out.println("[" + (readUncommitted ? "read_uncommitted" : "read_committed") + "]"
                            + " Partition: " + record.partition()
                            + "\tOffset: " + record.offset()
                            + "\tKey: " + record.key()
                            + "\tValue: " + record.value());
                }

                consumer.commitSync();
            }
        }
    }
}
