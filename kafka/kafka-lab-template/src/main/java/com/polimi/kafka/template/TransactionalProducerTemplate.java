package com.polimi.kafka.template;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * KAFKA TEMPLATE 3: Transactional Producer (Atomic Multi-Message Writes)
 *
 * Quando usarlo:
 * - Scrivere più record (anche su topic o partizioni diverse) in modo atomico (tutto-o-niente).
 * - I consumer con isolation.level = "read_committed" vedranno SOLO i record delle transazioni committate.
 *
 * Regole chiave:
 * - Impostare TRANSACTIONAL_ID_CONFIG (univoco per ogni istanza attiva, altrimenti Zombie Fencing).
 * - Chiamare producer.initTransactions() UNA sola volta prima del ciclo.
 * - Nota su abortTransaction(): chiamare producer.flush() prima di abortTransaction() forza l'invio sul log
 *   del broker prima del marker di abort (utile per testare read_committed vs read_uncommitted).
 *
 * Argomenti CLI opzionali:
 *   args[0] = topic (default: "inputTopic")
 *   args[1] = transactionalId (default: "txProducer-1")
 */
public class TransactionalProducerTemplate {
    private static final String DEFAULT_TOPIC = "inputTopic";
    private static final String DEFAULT_TRANSACTIONAL_ID = "txProducer-1";
    private static final int NUM_BATCHES = 50;
    private static final int WAIT_BETWEEN_MSGS_MS = 500;
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String topic = args.length >= 1 ? args[0] : DEFAULT_TOPIC;
        final String transactionalId = args.length >= 2 ? args[1] : DEFAULT_TRANSACTIONAL_ID;

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();

        try {
            for (int i = 0; i < NUM_BATCHES; i++) {
                final String key = "Key" + (i % 10);
                final String value = "TxVal" + i;

                try {
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(topic, key, value));

                    // Esempio didattico: committa i pari, abortisce i dispari
                    if (i % 2 == 0) {
                        producer.commitTransaction();
                        System.out.println("COMMITTED <" + key + ", " + value + ">");
                    } else {
                        // flush() invia i dati al broker prima del marker di abort (visibili solo a read_uncommitted)
                        producer.flush();
                        producer.abortTransaction();
                        System.out.println("ABORTED   <" + key + ", " + value + ">");
                    }
                } catch (ProducerFencedException | OutOfOrderSequenceException e) {
                    System.err.println("Fatal producer error (fenced or out of order): " + e.getMessage());
                    break;
                } catch (KafkaException e) {
                    System.err.println("Transient error, aborting transaction: " + e.getMessage());
                    producer.abortTransaction();
                }

                Thread.sleep(WAIT_BETWEEN_MSGS_MS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            producer.close();
        }
    }
}
