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
 * KAFKA TEMPLATE 5: Manual Commit Consumer (At-Least-Once Semantics)
 *
 * Quando usarlo:
 * - Vuoi garantire che nessun messaggio venga mai committato prima di essere stato elaborato (At-Least-Once).
 * - In caso di crash prima di commitSync(), i messaggi dopo l'ultimo commit verranno rielaborati al riavvio.
 *
 * Configurazione chiave:
 * - ENABLE_AUTO_COMMIT_CONFIG = false
 * - commitSync() chiamato dopo aver processato l'intero batch (o ogni COMMIT_EVERY messaggi).
 *
 * Argomenti CLI opzionali:
 *   args[0] = groupId (default: "manualGroup")
 *   args[1] = topic (default: "inputTopic")
 *   args[2] = offsetResetStrategy ("latest" | "earliest", default: "earliest")
 */
public class ManualCommitConsumerTemplate {
    private static final String DEFAULT_GROUP_ID = "manualGroup";
    private static final String DEFAULT_TOPIC = "inputTopic";
    private static final String DEFAULT_OFFSET_RESET = "earliest";
    private static final int COMMIT_EVERY = 10; // Imposta 1 per committare ogni messaggio, oppure usa commitSync() a fine batch
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String groupId = args.length >= 1 ? args[0] : DEFAULT_GROUP_ID;
        final String topic = args.length >= 2 ? args[1] : DEFAULT_TOPIC;
        final String offsetResetStrategy = args.length >= 3 ? args[2] : DEFAULT_OFFSET_RESET;

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            int numConsumed = 0;
            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                if (records.isEmpty()) {
                    continue;
                }

                for (final ConsumerRecord<String, String> record : records) {
                    // 1. Elabora prima il record (stampa / logica applicativa)
                    System.out.println("Partition: " + record.partition()
                            + "\tOffset: " + record.offset()
                            + "\tKey: " + record.key()
                            + "\tValue: " + record.value());

                    numConsumed++;
                    if (numConsumed >= COMMIT_EVERY) {
                        numConsumed = 0;
                        consumer.commitSync();
                        System.out.println("Committed offsets synchronously (every " + COMMIT_EVERY + " records)");
                    }
                }

                // Oppure: committa sempre alla fine di ogni batch elaborato con successo
                if (numConsumed > 0) {
                    numConsumed = 0;
                    consumer.commitSync();
                }
            }
        }
    }
}
