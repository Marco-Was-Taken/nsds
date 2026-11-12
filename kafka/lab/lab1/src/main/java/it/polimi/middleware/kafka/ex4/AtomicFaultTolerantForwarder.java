package it.polimi.middleware.kafka.ex4;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class AtomicFaultTolerantForwarder {
    private static final String defaultConsumerGroupId = "groupB";
    private static final String defaultInputTopic = "topicA";
    // Topic compattato: funge sia da output sia da changelog per ripristinare i contatori al riavvio
    private static final String defaultOutputTopic = "topicB";

    private static final String serverAddr = "localhost:9092";
    private static final String producerTransactionalId = "atomicCountForwarderId";

    public static void main(String[] args) {
        String consumerGroupId = args.length >= 1 ? args[0] : defaultConsumerGroupId;
        String inputTopic = args.length >= 2 ? args[1] : defaultInputTopic;
        String outputTopic = args.length >= 3 ? args[2] : defaultOutputTopic;

        // Consumer
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

        // Ripristino stato da outputTopic (assign + seekToBeginning per leggere sempre dall'inizio senza rebalance)
        final Map<String, Integer> countMap = new HashMap<>();
        final Properties storeProps = new Properties();
        storeProps.putAll(consumerProps);
        storeProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        try (KafkaConsumer<String, Integer> storeConsumer = new KafkaConsumer<>(storeProps)) {
            List<PartitionInfo> infos = storeConsumer.partitionsFor(outputTopic);
            if (infos != null && !infos.isEmpty()) {
                List<TopicPartition> partitions = infos.stream()
                        .map(p -> new TopicPartition(outputTopic, p.partition()))
                        .toList();
                storeConsumer.assign(partitions);
                storeConsumer.seekToBeginning(partitions);
                while (true) {
                    ConsumerRecords<String, Integer> stored = storeConsumer.poll(Duration.ofSeconds(1));
                    if (stored.isEmpty()) break;
                    stored.forEach(r -> countMap.put(r.key(), r.value()));
                }
            }
        }

        consumer.subscribe(Collections.singletonList(inputTopic));

        // Producer
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));

        final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
        producer.initTransactions();

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
            if (records.isEmpty()) continue;

            producer.beginTransaction();
            for (final ConsumerRecord<String, String> record : records) {
                final int newCount = countMap.compute(record.key(), (k, v) -> v == null ? 1 : v + 1);
                producer.send(new ProducerRecord<>(outputTopic, record.key(), newCount));
                System.out.println("Key: " + record.key() + "\tCount: " + newCount);
            }

            // The producer manually commits the offsets for the consumer within the transaction
            final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
            for (final TopicPartition partition : records.partitions()) {
                final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                map.put(partition, new OffsetAndMetadata(lastOffset + 1));
            }

            producer.sendOffsetsToTransaction(map, consumer.groupMetadata());
            producer.commitTransaction();
        }
    }
}
