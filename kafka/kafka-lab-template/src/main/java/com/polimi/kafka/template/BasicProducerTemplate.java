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
 * KAFKA TEMPLATE 2: Producer con Tutte le Garanzie Singolo-Messaggio (None / At-Most-Once / At-Least-Once / Exactly-Once)
 *
 * Nota critica su Kafka 3.5.1:
 *   Di default in Kafka 3.5.1 ENABLE_IDEMPOTENCE_CONFIG = "true", ACKS_CONFIG = "all", RETRIES_CONFIG = Integer.MAX_VALUE.
 *   Quindi, per disabilitare l'idempotenza (es. per None, At-Most-Once, At-Least-Once puro), va impostata ESPLICITAMENTE a "false"!
 *
 * Modalità supportate (args[1]):
 * - "none"         : acks="0", retries="0", enable.idempotence="false", send() asincrono fire-and-forget.
 *                    Nessuna garanzia: possibili perdite (e nessun attesa ACK).
 * - "at-most-once" : acks="1", retries="0", enable.idempotence="false".
 *                    Tenta l'invio 1 sola volta senza ritrasmissioni: mai duplicati, possibili perdite su errore/timeout.
 * - "at-least-once": acks="all", retries=MAX_VALUE, enable.idempotence="false", waitAck/flush.
 *                    Ritrasmette finché non riceve ACK: mai perdite, possibili duplicati se ACK si perde dopo scrittura.
 * - "exactly-once" : enable.idempotence="true" (implica acks="all", retries>0, max.in.flight<=5).
 *                    PID + sequence number: Exactly-Once tra Producer e singola Partizione.
 *                    (Per atomicità multi-messaggio/multi-topic usa invece TransactionalProducerTemplate).
 *
 * Modalità di invio mostrate:
 * - "async"    : producer.send(record) senza attendere
 * - "callback" : producer.send(record, (metadata, ex) -> ...)
 * - "sync"     : producer.send(record).get() bloccante (legge topic, partition, offset)
 * - "flush"    : invii asincroni + producer.flush() a fine batch
 *
 * Argomenti CLI opzionali:
 *   args[0] = topic (default: "inputTopic")
 *   args[1] = guarantee ("none" | "at-most-once" | "at-least-once" | "exactly-once" | "true" | "false", default: "exactly-once")
 *   args[2] = sendMode ("sync" | "async" | "callback" | "flush", default: "sync")
 */
public class BasicProducerTemplate {
    private static final String DEFAULT_TOPIC = "inputTopic";
    private static final String DEFAULT_GUARANTEE = "exactly-once";
    private static final String DEFAULT_SEND_MODE = "sync";
    private static final int NUM_MESSAGES = 1000;
    private static final int WAIT_BETWEEN_MSGS_MS = 500;
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String topic = args.length >= 1 ? args[0] : DEFAULT_TOPIC;
        final String rawGuarantee = args.length >= 2 ? args[1].toLowerCase() : DEFAULT_GUARANTEE;
        final String sendMode = args.length >= 3 ? args[2].toLowerCase() : DEFAULT_SEND_MODE;

        // Compatibilità con vecchio argomento booleano ("true" -> exactly-once, "false" -> at-least-once)
        final String guarantee;
        if ("true".equals(rawGuarantee)) {
            guarantee = "exactly-once";
        } else if ("false".equals(rawGuarantee)) {
            guarantee = "at-least-once";
        } else {
            guarantee = rawGuarantee;
        }

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        switch (guarantee) {
            case "none":
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(false));
                props.put(ProducerConfig.ACKS_CONFIG, "0");
                props.put(ProducerConfig.RETRIES_CONFIG, "0");
                break;
            case "at-most-once":
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(false));
                props.put(ProducerConfig.ACKS_CONFIG, "1");
                props.put(ProducerConfig.RETRIES_CONFIG, "0");
                break;
            case "at-least-once":
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(false));
                props.put(ProducerConfig.ACKS_CONFIG, "all");
                props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
                break;
            case "exactly-once":
            default:
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));
                props.put(ProducerConfig.ACKS_CONFIG, "all");
                break;
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            final Random r = new Random();

            for (int i = 0; i < NUM_MESSAGES; i++) {
                // Con key esplicita -> stessa key = stessa partizione = ordine FIFO garantito
                // Senza key (new ProducerRecord<>(topic, value)) -> bilanciamento senza località
                final String key = "Key" + r.nextInt(10);
                final String value = "Val" + i;
                final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                if ("callback".equals(sendMode)) {
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Send failed for <" + key + ", " + value + ">: " + exception.getMessage());
                        } else {
                            System.out.println("Callback ACK <" + key + ", " + value + "> @ partition "
                                    + metadata.partition() + ", offset " + metadata.offset());
                        }
                    });
                } else {
                    final Future<RecordMetadata> future = producer.send(record);
                    if ("sync".equals(sendMode) && !"none".equals(guarantee)) {
                        try {
                            final RecordMetadata ack = future.get();
                            System.out.println("[" + guarantee + "] Sent <" + key + ", " + value + "> to " + ack.topic()
                                    + " @ partition " + ack.partition() + ", offset " + ack.offset());
                        } catch (InterruptedException | ExecutionException e) {
                            System.err.println("[" + guarantee + "] Error sending <" + key + ", " + value + ">: " + e.getMessage());
                        }
                    } else if ("flush".equals(sendMode) && i % 10 == 9) {
                        producer.flush();
                    }
                }

                try {
                    Thread.sleep(WAIT_BETWEEN_MSGS_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            if ("flush".equals(sendMode)) {
                producer.flush();
            }
        }
    }
}
