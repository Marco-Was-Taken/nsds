package com.polimi.kafka.template;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * KAFKA TEMPLATE: Windowed Transactional Forwarder (Tumbling Window + EOS — Rif. Exam 2024 Consumer1)
 *
 * Trappole d'esame evitate da questo template:
 * 1. La lista `window` è dichiarata FUORI dal ciclo `while (true)` (poll() ritorna appena c'è >= 1 record,
 *    non aspetta la durata del timeout per riempire la finestra!).
 * 2. `beginTransaction()` viene aperto SOLO quando `window.size() == WINDOW_SIZE`, MAI ad ogni `poll()`.
 * 3. `sendOffsetsToTransaction(...)` committa ESCLUSIVAMENTE gli offset dei record contenuti nella finestra
 *    appena emessa (e MAI eventuali record parziali successivi del poll!).
 * 4. Nota architetturale: una finestra GLOBALE cross-key richiede 1 sola istanza (min=1, max=1).
 *    Per una finestra PER CHIAVE con più istanze nello stesso groupId, mantieni `Map<String, List<ConsumerRecord<...>>>`
 *    fuori dal while(true) e usa transactionalId univoci per ciascuna istanza!
 *
 * Argomenti CLI opzionali:
 *   args[0] = transactionalId (default: "windowedForwarderTx-1")
 *   args[1] = groupId (default: "windowedForwarderGroup")
 *   args[2] = inputTopic (default: "inputTopic")
 *   args[3] = outputTopic (default: "outputTopic")
 */
public class WindowedForwarderTemplate {
    private static final String DEFAULT_TRANSACTIONAL_ID = "windowedForwarderTx-1";
    private static final String DEFAULT_GROUP_ID = "windowedForwarderGroup";
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";
    private static final int WINDOW_SIZE = 10;
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) {
        final String transactionalId = args.length >= 1 ? args[0] : DEFAULT_TRANSACTIONAL_ID;
        final String groupId = args.length >= 2 ? args[1] : DEFAULT_GROUP_ID;
        final String inputTopic = args.length >= 3 ? args[2] : DEFAULT_INPUT_TOPIC;
        final String outputTopic = args.length >= 4 ? args[3] : DEFAULT_OUTPUT_TOPIC;

        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));

        try (KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Collections.singletonList(inputTopic));
            producer.initTransactions();

            // CRITICO: la finestra vive FUORI dal while(true)
            final List<ConsumerRecord<String, Integer>> window = new ArrayList<>(WINDOW_SIZE);

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, Integer> record : records) {
                    window.add(record);

                    if (window.size() == WINDOW_SIZE) {
                        try {
                            producer.beginTransaction();

                            int sum = 0;
                            final Map<TopicPartition, OffsetAndMetadata> windowOffsets = new HashMap<>();
                            for (final ConsumerRecord<String, Integer> winRecord : window) {
                                sum += winRecord.value();
                                final TopicPartition tp = new TopicPartition(winRecord.topic(), winRecord.partition());
                                windowOffsets.merge(
                                        tp,
                                        new OffsetAndMetadata(winRecord.offset() + 1),
                                        (oldVal, newVal) -> newVal.offset() > oldVal.offset() ? newVal : oldVal
                                );
                            }

                            producer.send(new ProducerRecord<>(outputTopic, "sum", sum));
                            producer.sendOffsetsToTransaction(windowOffsets, consumer.groupMetadata());
                            producer.commitTransaction();

                            System.out.println("[WindowedEOS] Emitted sum=" + sum + " and committed window offsets.");
                            window.clear();
                        } catch (ProducerFencedException | OutOfOrderSequenceException e) {
                            System.err.println("Fatal fencing/sequence error: " + e.getMessage());
                            return;
                        } catch (KafkaException e) {
                            System.err.println("Window transaction aborted, rewinding to window start: " + e.getMessage());
                            producer.abortTransaction();
                            // Ripristina l'offset della prima occorrenza di ciascuna partizione nella finestra
                            final Map<TopicPartition, Long> firstOffsets = new HashMap<>();
                            for (final ConsumerRecord<String, Integer> winRecord : window) {
                                final TopicPartition tp = new TopicPartition(winRecord.topic(), winRecord.partition());
                                firstOffsets.putIfAbsent(tp, winRecord.offset());
                            }
                            for (final Map.Entry<TopicPartition, Long> entry : firstOffsets.entrySet()) {
                                if (consumer.assignment().contains(entry.getKey())) {
                                    consumer.seek(entry.getKey(), entry.getValue());
                                }
                            }
                            window.clear();
                            break; // Esce dal for sul poll corrente per rileggere dal punto di seek
                        }
                    }
                }
            }
        }
    }
}
