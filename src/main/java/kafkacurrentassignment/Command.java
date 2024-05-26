package kafkacurrentassignment;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import joptsimple.OptionSpec;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Command {
    public static void main(String... args) throws Exception {
        Options options = new Options(args);
        execute(options);
    }

    private static void execute(Options options, String... args) throws Exception {
        try (Admin adminClient = createAdminClient(options)) {
            execute(options, adminClient);
        }
    }

    private static Admin createAdminClient(Options options) throws IOException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers());
        props.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "log-dirs-tool");
        return Admin.create(props);
    }

    static void execute(Options options, Admin adminClient) throws Exception {
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
            if (!options.topics().isEmpty() && !options.topics().contains(topicPartition.topic())) {
                return;
            }

            boolean containBroker = replicaWithLogDirs.stream().anyMatch(r -> {
                return options.brokers().contains(r.id);
            });
            containBroker |= options.brokers().isEmpty();
            if (!containBroker) {
                return;
            }

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

    private static final class replicaWithLogDir {
        private String logDir;
        private int id;
        replicaWithLogDir(int id, String lodDir) {
            this.logDir = lodDir;
            this.id = id;
        }
    }

    // ref. https://github.com/apache/kafka/blob/d9ee9c96dd5081f3b494f5fc820fb3f51328993f/tools/src/main/java/org/apache/kafka/tools/LogDirsCommand.java#L157
    // Visible for testing
    static class Options extends CommandDefaultOptions {
        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<String> topicListOpt;
        private final OptionSpec<String> brokerListOpt;

        public Options(String... args) {
            super(args);

            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: the server(s) to use for bootstrapping")
                    .withRequiredArg()
                    .describedAs("The server(s) to use for bootstrapping")
                    .ofType(String.class);
            topicListOpt = parser.accepts("topic-list", "The list of topics to be queried in the form \"topic1,topic2,topic3\". " +
                            "All topics will be queried if no topic list is specified")
                    .withRequiredArg()
                    .describedAs("Topic list")
                    .defaultsTo("")
                    .ofType(String.class);
            brokerListOpt = parser.accepts("broker-list", "The list of brokers to be queried in the form \"0,1,2\". " +
                            "All brokers in the cluster will be queried if no broker list is specified")
                    .withRequiredArg()
                    .describedAs("Broker list")
                    .ofType(String.class)
                    .defaultsTo("");

            options = parser.parse(args);

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to query log directory usage on the specified brokers.");
        }

        private Stream<String> splitAtCommasAndFilterOutEmpty(OptionSpec<String> option) {
            return Arrays.stream(options.valueOf(option).split(",")).filter(x -> !x.isEmpty());
        }

        private String bootstrapServers() {
            return options.valueOf(bootstrapServerOpt);
        }

        private Set<String> topics() {
            return splitAtCommasAndFilterOutEmpty(topicListOpt).collect(Collectors.toSet());
        }

        private Set<Integer> brokers() {
            return splitAtCommasAndFilterOutEmpty(brokerListOpt).map(Integer::valueOf).collect(Collectors.toSet());
        }
    }
}