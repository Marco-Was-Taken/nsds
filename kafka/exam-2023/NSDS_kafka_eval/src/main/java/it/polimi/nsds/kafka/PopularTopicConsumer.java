package it.polimi.nsds.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class PopularTopicConsumer {
    private static final String topic = "inputTopic";
    private static final String serverAddr = "localhost:9092";


    public static void main(String[] args) {
        String groupId = args[0];

        HashMap<String, Integer> popularTopics = new HashMap<>();

        final Properties propsConsumer = new Properties();
        propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        propsConsumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(propsConsumer);

        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, Integer> record : records) {
                String topicName = record.topic();

                popularTopics.put(topicName, popularTopics.getOrDefault(topicName, 0) + 1);

                System.out.println("The most popular topic is: " + mostPopularTopic(popularTopics));
            }
        }
    }

    private static String mostPopularTopic(Map<String, Integer> popularTopics){
        return popularTopics
                .keySet()
                .stream()
                .max(Comparator.comparingInt(popularTopics::get))
                .orElse(null);
    }
}
