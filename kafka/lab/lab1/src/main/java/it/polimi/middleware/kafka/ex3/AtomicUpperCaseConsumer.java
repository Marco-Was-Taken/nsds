package it.polimi.middleware.kafka.ex3;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * Exercise 3: Consumer C2 con semantica Exactly-Once (EOS) stateless.
 * - Ogni messaggio di input viene elaborato e consegnato a topicB esattamente una volta.
 * - Con più istanze nello stesso gruppo ("groupA"): ogni istanza deve avere un transactionalId
 *   UNIVOCO (es. passato da args[0]), altrimenti la nuova istanza fa zombie-fencing sulla precedente.
 */
public class AtomicUpperCaseConsumer {
    private static final String defaultGroupId = "groupA";
    private static final String inputTopic = "topicA";
    private static final String outputTopic = "topicB";
    private static final String defaultTransactionalId = "ex3Forwarder";
    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args) {
        String transactionalId = args.length >= 1 ? args[0] : defaultTransactionalId;
        String groupId = args.length >= 2 ? args[1] : defaultGroupId;

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // CRITICO (EOS): auto-commit disabilitato e isolation.level = read_committed
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(inputTopic));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        producer.initTransactions();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(5));
            if (records.isEmpty()) continue;

            try {
                producer.beginTransaction();
                for (ConsumerRecord<String, String> record : records) {
                    String value = record.value().toLowerCase();
                    producer.send(new ProducerRecord<>(outputTopic, record.key(), value));
                }

                // Committa atomicamente record su outputTopic + offset di inputTopic (lastOffset + 1)
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                }

                producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                producer.commitTransaction();
            } catch (ProducerFencedException | OutOfOrderSequenceException e) {
                producer.close();
                consumer.close();
                break;
            } catch (KafkaException e) {
                producer.abortTransaction();
                for (TopicPartition partition : records.partitions()) {
                    if (consumer.assignment().contains(partition)) {
                        consumer.seek(partition, records.records(partition).get(0).offset());
                    }
                }
            }
        }
    }
}
