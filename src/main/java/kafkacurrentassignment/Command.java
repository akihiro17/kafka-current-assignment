package kafkacurrentassignment;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class Command {
    public static void main(String... args) {
        System.out.println("Hello IntelliJ!!");
    }

    private static void execute(String... args) throws Exception {
        try (Admin adminClient = createAdminClient()) {
            execute(adminClient);
        }
    }

    private static Admin createAdminClient() throws IOException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1");
        props.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "log-dirs-tool");
        return Admin.create(props);
    }

    static void execute(Admin adminClient) throws Exception {
        DescribeClusterResult cluster = adminClient.describeCluster();
        System.out.println("Connected to cluster " + cluster.clusterId().get());
        System.out.println("The brokers in the cluster are:");
        cluster.nodes().get().forEach(node -> System.out.println("* " + node));
        System.out.println("The controller is: " + cluster.controller().get());
        List<Integer> brokers = cluster.nodes().get().stream().map(Node::id).collect(Collectors.toList());
        DescribeLogDirsResult logDirs = adminClient.describeLogDirs(brokers);
        logDirs.allDescriptions().get().forEach((k,v) -> {
            System.out.println("key is :" + k);
            v.forEach((k1,v1) -> {
                System.out.println("key1 is :" + k1);
                System.out.println("value1 is :" + v1);
            });
        });
    }
}