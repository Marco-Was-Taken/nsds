package it.polimi.middleware.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class TopicManager {
    private static final String defaultTopicName = "topicA";
    private static final int defaultTopicPartitions = 2;
    private static final short defaultReplicationFactor = 1;

    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args) throws Exception {
        final String topicName = args.length >= 1 ? args[0] : defaultTopicName;
        final int topicPartitions = args.length >= 2 ? Integer.parseInt(args[1]) : defaultTopicPartitions;
        final short replicationFactor = args.length >= 3 ? Short.parseShort(args[2]) : defaultReplicationFactor;
        final boolean compact = args.length >= 4 && Boolean.parseBoolean(args[3]);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsResult listResult = adminClient.listTopics();
            Set<String> topicsNames = listResult.names().get();
            System.out.println("Available topics: " + topicsNames);

            if (topicsNames.contains(topicName)) {
                System.out.println("Deleting topic " + topicName);
                DeleteTopicsResult delResult = adminClient.deleteTopics(Collections.singletonList(topicName));
                delResult.all().get();
                System.out.println("Done!");
                // Wait for the deletion
                Thread.sleep(5000);
            }

            NewTopic newTopic = new NewTopic(topicName, topicPartitions, replicationFactor);
            if (compact) {
                Map<String, String> topicConfigs = new HashMap<>();
                topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
                topicConfigs.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01");
                topicConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, "5000");
                newTopic.configs(topicConfigs);
            }

            System.out.println("Adding topic " + topicName + " with " + topicPartitions + " partitions (compact=" + compact + ")");
            CreateTopicsResult createResult = adminClient.createTopics(Collections.singletonList(newTopic));
            createResult.all().get();
            System.out.println("Done!");
        }
    }
}