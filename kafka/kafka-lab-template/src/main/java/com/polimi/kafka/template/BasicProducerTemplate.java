package com.polimi.kafka.template;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * KAFKA TEMPLATE 2: Basic & Idempotent Producer (Async vs Sync ACK)
 *
 * Concetti chiave:
 * - Chiave (key): messaggi con la stessa key finiscono sempre nella stessa partizione (ordine FIFO per key).
 *   Senza key (null), il broker bilancia tra le partizioni senza località per chiave.
 * - waitAck = false: invio asincrono (massimo throughput).
 * - waitAck = true: future.get() attende l'ACK del broker e legge partition + offset assegnati.
 * - enableIdempotence = true: evita duplicati tra producer e singola partizione in caso di retry (Exactly-Once Producer->Partition).
 *
 * Argomenti CLI opzionali:
 *   args[0] = topic (default: "inputTopic")
 *   args[1] = idempotent ("true" | "false", default: "false")
 */
public class BasicProducerTemplate {
    private static final String DEFAULT_TOPIC = "inputTopic";
    private static final int NUM_MESSAGES = 1000;
    private static final int WAIT_BETWEEN_MSGS_MS = 500;
    private static final boolean WAIT_ACK = true;
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String topic = args.length >= 1 ? args[0] : DEFAULT_TOPIC;
        final boolean idempotent = args.length >= 2 && Boolean.parseBoolean(args[1]);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(idempotent));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            final Random r = new Random();

            for (int i = 0; i < NUM_MESSAGES; i++) {
                final String key = "Key" + r.nextInt(10);
                final String value = "Val" + i;

                final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                final Future<RecordMetadata> future = producer.send(record);

                if (WAIT_ACK) {
                    try {
                        final RecordMetadata ack = future.get();
                        System.out.println("Sent <" + key + ", " + value + "> to " + ack.topic()
                                + " @ partition " + ack.partition() + ", offset " + ack.offset());
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }

                try {
                    Thread.sleep(WAIT_BETWEEN_MSGS_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
}
