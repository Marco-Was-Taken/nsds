package com.polimi.kafka.template;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * KAFKA TEMPLATE 1: Topic Management & Log Compaction (AdminClient)
 *
 * Quando usarlo:
 * - Creare o ricreare (reset) un topic con un numero specifico di partizioni (es. per parallelizzare i consumer).
 * - Configurare un topic di output/changelog con Log Compaction (es. per forwarder stateful).
 *
 * Argomenti CLI opzionali:
 *   args[0] = topicName (default: "inputTopic")
 *   args[1] = numPartitions (default: 2)
 *   args[2] = replicationFactor (default: 1)
 *   args[3] = compact ("true" | "false", default: "false")
 */
public class TopicAdminTemplate {
    private static final String DEFAULT_TOPIC_NAME = "inputTopic";
    private static final int DEFAULT_PARTITIONS = 2;
    private static final short DEFAULT_REPLICATION_FACTOR = 1;
    private static final String SERVER_ADDR = "localhost:9092";

    public static void main(String[] args) throws Exception {
        final String topicName = args.length >= 1 ? args[0] : DEFAULT_TOPIC_NAME;
        final int numPartitions = args.length >= 2 ? Integer.parseInt(args[1]) : DEFAULT_PARTITIONS;
        final short replicationFactor = args.length >= 3 ? Short.parseShort(args[2]) : DEFAULT_REPLICATION_FACTOR;
        final boolean compact = args.length >= 4 && Boolean.parseBoolean(args[3]);

        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // 1. Elenca i topic esistenti
            final ListTopicsResult listResult = adminClient.listTopics();
            final Set<String> topicNames = listResult.names().get();
            System.out.println("Available topics: " + topicNames);

            // 2. Se il topic esiste già, lo elimina e attende il completamento sul broker
            if (topicNames.contains(topicName)) {
                System.out.println("Deleting existing topic: " + topicName);
                final DeleteTopicsResult delResult = adminClient.deleteTopics(Collections.singletonList(topicName));
                delResult.all().get();
                Thread.sleep(5000);
            }

            // 3. Definisce il nuovo topic con numero di partizioni e replication factor
            final NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);

            // 4. Opzionale: Log Compaction (mantiene solo l'ultimo valore committato per ciascuna key)
            if (compact) {
                final Map<String, String> topicConfigs = new HashMap<>();
                topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
                topicConfigs.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01");
                topicConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, "5000");
                newTopic.configs(topicConfigs);
            }

            System.out.println("Creating topic " + topicName + " (partitions=" + numPartitions + ", compact=" + compact + ")");
            final CreateTopicsResult createResult = adminClient.createTopics(Collections.singletonList(newTopic));
            createResult.all().get();
            System.out.println("Done!");
        }
    }
}
