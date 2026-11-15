package it.polimi.nsds.kafka;

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
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class AtMostOncePrinter {
    private static final String topic = "inputTopic";
    private static final String serverAddr = "localhost:9092";
    private static final int threshold = 500;

    public static void main(String[] args) {
        String groupId = args[0];

        final Properties propsConsumer = new Properties();
        propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        propsConsumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(propsConsumer);
        consumer.subscribe(Collections.singletonList(topic));

        while(true){
            ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMinutes(1));

            if(records.isEmpty()){
                continue;
            }

            for (ConsumerRecord<String, Integer> record : records) {
                final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                final Map<TopicPartition, OffsetAndMetadata> singleOffset =
                        Collections.singletonMap(tp, new OffsetAndMetadata(record.offset() + 1));
                consumer.commitSync(singleOffset);

                if(record.value() >= threshold){
                    System.out.println(record.key() + " " + record.value());
                }
            }
        }

    }
}
