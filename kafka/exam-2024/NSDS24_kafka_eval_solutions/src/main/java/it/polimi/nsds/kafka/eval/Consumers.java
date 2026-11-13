package it.polimi.nsds.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

// Group number:
// Group members:

// Number of partitions for inputTopic (min, max):
// Number of partitions for outputTopic1 (min, max):
// Number of partitions for outputTopic2 (min, max):

// Number of instances of Consumer1 (and groupId of each instance) (min, max):
// Number of instances of Consumer2 (and groupId of each instance) (min, max):

// Please, specify below any relation between the number of partitions for the topics
// and the number of instances of each Consumer

public class Consumers {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int consumerId = Integer.valueOf(args[0]);
        String groupId = args[1];
        if (consumerId == 1) {
            Consumer1 consumer = new Consumer1(serverAddr, groupId);
            consumer.execute();
        } else if (consumerId == 2) {
            Consumer2 consumer = new Consumer2(serverAddr, groupId);
            consumer.execute();
        }
    }

    private static class Consumer1 {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String transactionId = "consumer1Id";

        private static final String inputTopic = "inputTopic";
        private static final String outputTopic = "outputTopic1";

        public Consumer1(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            // The consumer does not commit automatically, but within the producer transaction
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
            producer.initTransactions();

            final List<ConsumerRecord<String, Integer>> window = new ArrayList<>(10);
            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, Integer> record : records) {
                    final String key = record.key();
                    final Integer value = record.value();
                    System.out.println("Received key: " + key + " value: " + value);
                    window.add(record);
                    if (window.size() == 10) {
                        System.out.println("Window full");
                        producer.beginTransaction();

                        // The producer manually commits the offsets for the consumer within the transaction
                        final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                        int sum = 0;
                        for (final ConsumerRecord<String, Integer> winRecord : window) {
                            sum += winRecord.value();
                            producer.send(new ProducerRecord<>(outputTopic, key, value));
                            map.put(new TopicPartition(winRecord.topic(), winRecord.partition()), new OffsetAndMetadata(winRecord.offset() + 1));
                        }

                        // The producer sends the sum downstream
                        System.out.println("Sum of values: " + sum);
                        System.out.println("Sending to output topic: " + outputTopic);
                        producer.send(new ProducerRecord<>(outputTopic, "Sum", sum));
                        producer.sendOffsetsToTransaction(map, consumer.groupMetadata());
                        producer.commitTransaction();

                        window.clear();
                    }
                }
            }
        }
    }

    private static class Consumer2 {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String inputTopic = "inputTopic";
        private static final String outputTopic = "outputTopic2";

        public Consumer2(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(true));

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(inputTopic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            final Map<String, Integer> windowSums = new HashMap<>();
            final Map<String, Integer> windowCount = new HashMap<>();
            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, Integer> record : records) {
                    final String key = record.key();
                    final Integer value = record.value();
                    System.out.println("Received key: " + key + " value: " + value);
                    windowSums.put(key, windowSums.getOrDefault(key, 0) + value);
                    windowCount.put(key, windowCount.getOrDefault(key, 0) + 1);
                    if (windowCount.get(key) == 10) {
                        System.out.println("Window full");
                        producer.send(new ProducerRecord<>(outputTopic, key, windowSums.get(key)));
                        windowSums.remove(key);
                        windowCount.remove(key);
                    }
                }
            }
        }
    }
}