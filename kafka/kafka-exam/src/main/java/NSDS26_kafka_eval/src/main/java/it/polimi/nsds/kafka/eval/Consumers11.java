 package NSDS26_kafka_eval.src.main.java.it.polimi.nsds.kafka.eval;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

// Group number:
// Group members:

// How many partitions can the topics "temperature" and "humidity" have?
// There's not a maximum number of partitions, given the assumptions we have

// Is there any relation between the number of partitions in "temperature" and "humidity"?
// Also given the assumption we have, we need the same amount of partitions between the two topics, as it guarantees that every consumer receives the values for the keys.
 // Especially important if one of the two topics had fewer partitions than the number of consumers in the group.

// Is it possible to have more than one instance of Monitor?
// If so, what is the relation between their group id?

//Yes, if they have different group ids they you can have as many as you want, they are independent.
//If they have same group ids, you need the topics to have the same amount of partitions, so that all the same keys are distributed deterministically to the various consumers (as in the assummptions)
//In that case you also can have how many consumers as you want, but more than the number of partitions will just be wasting resources as they don't get any message.


// How many partitions can the topic "alerts" have?
//It doesn't really matter, the consumer doesn't do anything depending from different partitions.

// Is it possible to have more than one instance of Reactor?
// If so, what is the relation between their group id?

//With the same group id you can have them, same thing as before (waste of resources etc etc), also we don't really are about the number of partitions of the topic as we don't do any operations based on that.
//With different group ids our implementation we would have to change the implementation of the storeTopic for fault tolerance to support multiple Reactors consumers.


// How many partitions can the topic "processedAlerts" have?
//Any number, we don't really care or need it other than sending it.

// Which additional topics, if any, do you use? How many partitions can they have?
//Just once, called storeTopic, to save the state of the single Reactor operation. We could have more than one if we explicitly set a partition so that different group ids pull from different partitions of the topic.

public class Consumers11 {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int stage = Integer.parseInt(args[0]);
        String groupId = args[1];
        // TODO: add arguments if necessary
        switch (stage) {
            case 1:
                new Monitor(serverAddr, groupId).execute();
                break;
            case 2:
                new Reactor(serverAddr, groupId).execute();
                break;
            default:
                System.err.println("Wrong stage");
        }
    }

    // Simulates the first reaction of the system to an alert (provided, do not modify)
    private static void reaction1(String room, String alert) {
        System.out.println("[reaction1] room=" + room + " " + alert);
    }

    // Simulates the second reaction of the system to an alert (provided, do not modify)
    private static void reaction2(String room, String alert) {
        System.out.println("[reaction2] room=" + room + " " + alert);
    }

    private static class Monitor {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String temperaturesTopic = "temperature";
        private static final String humiditiesTopic = "humidity";
        private static final String outputTopic = "alerts";

        private static final int windowSize = 3;
        private static final double temperatureThreshold = 28;
        private static final double humidityThreshold = 40;

        // TODO: add attributes if necessary

        // TODO: add arguments if necessary
        public Monitor(String serverAddr, String consumerGroupId) {
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
            consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());

            // TODO: add properties if needed

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);

            consumer.subscribe(Collections.singletonList(temperaturesTopic));
            consumer.subscribe(Collections.singletonList(humiditiesTopic));


            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // TODO: add properties if needed

            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

            HashMap<String,List<Integer>> mapTemperature = new HashMap();
            HashMap<String,List<Integer>> mapHumidity = new HashMap();

            // TODO: implement the processing logic
            while (true) {
                // TODO: implement the processing logic

                ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMinutes(2));

                for (ConsumerRecord<String, Integer> record : records) {
                    List<Integer> temperatureValues = mapTemperature.get(record.key());
                    if (temperatureValues == null) {
                        temperatureValues = new LinkedList<>();
                        mapTemperature.put(record.key(), temperatureValues);
                        continue;
                    }

                    List<Integer> humidityvalues = mapHumidity.get(record.key());
                    if (humidityvalues == null) {
                        humidityvalues = new LinkedList<>();
                        mapHumidity.put(record.key(), humidityvalues);
                        continue;
                    }


                    temperatureValues.add(record.value());
                    humidityvalues.add(record.value());


                    if (temperatureValues.size() >= 3 && humidityvalues.size() >= 3) {

                        Integer temperatureAverage = calculateAverage(temperatureValues.subList(0, 3));
                        Integer humidityAverage = calculateAverage(humidityvalues.subList(0, 3));

                        if(temperatureAverage > 28 && humidityAverage < 40) {
                            producer.send(new ProducerRecord<String,String>(outputTopic, record.key(), record.key() + " alerts send " + temperatureAverage + " " + humidityAverage));

                           ;

                        }

                        temperatureValues.remove(0);
                        humidityvalues.remove(0);
                    }


                }

            }
        }


        private final Integer calculateAverage(List<Integer> values) {
            int sum = 0;
            for (Integer value : values) {
                sum += value;
            }
            return sum / values.size();
        }


    }

    private static class Reactor {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String inputTopic = "alerts";
        private static final String outputTopic = "processedAlerts";
        private static final String storeTopic = "store";

        // TODO: add attributes if needed

        // TODO: add arguments if necessary
        public Reactor(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            ConsumerRecord<String, String> lastRecordStored = null;
            long lastOffsetStored = -1;

            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // TODO: add properties if needed
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

            consumer.subscribe(Collections.singletonList(inputTopic));
            consumer.subscribe(Collections.singletonList(storeTopic));


            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


            // TODO: add properties if needed
            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
            // TODO: implement the processing logic
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(5));

                for(ConsumerRecord<String,String> record : records)     {
                    if(Objects.equals(record.topic(), storeTopic)) {
                          lastRecordStored = record;
                          lastOffsetStored = record.offset();
                    }
                }

                for(ConsumerRecord<String,String> record : records) {
                    if(record.topic().equals(storeTopic)) {
                        continue;
                    }

                    if(lastRecordStored == null || lastRecordStored.key().equals(record.key()) && lastRecordStored.value() == "0") {
                        reaction1(record.key(), record.value());
                    }

                    producer.send(new ProducerRecord<String,String>(storeTopic, record.key(), Integer.toString(1)));

                    producer.flush();
                    if(lastRecordStored != null) {
                        final TopicPartition tp = new TopicPartition(storeTopic, 0);
                        lastOffsetStored++;
                        final Map<TopicPartition, OffsetAndMetadata> singleOffset =
                                Collections.singletonMap(tp, new OffsetAndMetadata(lastOffsetStored));

                        consumer.commitSync(singleOffset);
                    }


                      if(lastRecordStored == null || lastRecordStored.key().equals(record.key()) && lastRecordStored.value() == "1") {
                          reaction2(record.key(), record.value());
                      }



                    producer.send(new ProducerRecord<String,String>(storeTopic, record.key(), Integer.toString(2)));

                    producer.flush();

                    if(lastRecordStored != null) {
                           final TopicPartition tp2 = new TopicPartition(storeTopic, 0);
                           lastOffsetStored++;
                           final Map<TopicPartition, OffsetAndMetadata> singleOffset2 =
                           Collections.singletonMap(tp2, new OffsetAndMetadata(lastOffsetStored));

                           consumer.commitSync(singleOffset2);

                    }

                    producer.send(new ProducerRecord<>(outputTopic, record.key(), record.value()));

                    producer.flush();

                    final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    final Map<TopicPartition, OffsetAndMetadata> singleOffset =
                            Collections.singletonMap(tp, new OffsetAndMetadata(record.offset() + 1));
                    consumer.commitSync(singleOffset);

                }
            }
        }
    }


}
