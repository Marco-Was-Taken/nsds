package it.polimi.middleware.kafka.eval;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class Producer {
    private static final String topic1 = "sensors1";
    private static final String topic2 = "sensors2";

    private static final int waitBetweenMsgs = 1000;

    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

        final KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        final Random r = new Random();

        // Continuously emit random integer values for keys Key0..Key9 onto sensors1 or sensors2
        while (true) {
            final String topic = r.nextBoolean() ? topic1 : topic2;
            final String key = "Key" + r.nextInt(10);
            final int value = r.nextInt();
            System.out.println(
                    "Topic: " + topic +
                    "\tKey: " + key +
                    "\tValue: " + value
            );

            final ProducerRecord<String, Integer> record = new ProducerRecord<>(topic, key, value);
            producer.send(record);

            try {
                Thread.sleep(waitBetweenMsgs);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}