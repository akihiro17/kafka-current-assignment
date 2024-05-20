package kafkacurrentassignment;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.stream.Collectors;

public class Command {
    public static void main(String... args) throws Exception {
        execute();
    }

    private static void execute(String... args) throws Exception {
        try (Admin adminClient = createAdminClient()) {
            execute(adminClient);
        }
    }

    private static Admin createAdminClient() throws IOException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8097");
        props.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "log-dirs-tool");
        return Admin.create(props);
    }

    private static final class replicaWithLogDir {
        private String logDir;
        private int id;
        replicaWithLogDir(int id, String lodDir) {
            this.logDir = lodDir;
            this.id = id;
        }
    }

    static void execute(Admin adminClient) throws Exception {
        DescribeClusterResult cluster = adminClient.describeCluster();

        List<Integer> brokers = cluster.nodes().get().stream().map(Node::id).collect(Collectors.toList());
        Map<TopicPartition, List<replicaWithLogDir>> topicPartitionListHashMap = new HashMap<>();

        DescribeLogDirsResult logDirs = adminClient.describeLogDirs(brokers);
        logDirs.allDescriptions().get().forEach((brokerId, logDirDescriptionMap) -> {
            logDirDescriptionMap.forEach((directoryName, logDirDescription) -> {
                logDirDescription.replicaInfos().forEach((topicPartition, replicaInfo) -> {
                    if (!topicPartitionListHashMap.containsKey(topicPartition)) {
                        topicPartitionListHashMap.put(topicPartition, new ArrayList<>());
                    }
                    topicPartitionListHashMap.get(topicPartition).add(
                        new replicaWithLogDir(brokerId, directoryName)
                    );
                });
            });
        });

        Assignment assignment = new Assignment();
        assignment.setVersion(1);
        List<Partition> partitions = new ArrayList<>();

        topicPartitionListHashMap.forEach(((topicPartition, replicaWithLogDirs) -> {
            Partition p = new Partition();
            p.setTopic(topicPartition.topic());
            p.setPartition(topicPartition.partition());
            p.setReplicas(
                replicaWithLogDirs.stream().map(v -> v.id).collect(Collectors.toList())
            );
            p.setLogDirs(
                replicaWithLogDirs.stream().map(v -> v.logDir).collect(Collectors.toList())
            );
            partitions.add(p);
        }));
        assignment.setPartitions(partitions);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(assignment);

        System.out.println(json);
    }
}