package kafkacurrentassignment;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
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

    static String buildAssignment(Options options, Admin adminClient) throws Exception {
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

        // Get all unique topics from the topicPartitionListHashMap
        Set<String> topicNames = topicPartitionListHashMap.keySet().stream()
            .map(TopicPartition::topic)
            .collect(Collectors.toSet());

        // Get topic descriptions to obtain correct replica ordering
        DescribeTopicsResult topicsResult = adminClient.describeTopics(topicNames);
        Map<String, TopicDescription> topicDescriptions = topicsResult.allTopicNames().get();

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

            // Get the correct replica ordering from topic description
            TopicDescription topicDescription = topicDescriptions.get(topicPartition.topic());
            List<Integer> correctReplicaOrder = topicDescription.partitions().stream()
                .filter(partitionInfo -> partitionInfo.partition() == topicPartition.partition())
                .findFirst()
                .map(partitionInfo -> partitionInfo.replicas().stream()
                    .map(Node::id)
                    .collect(Collectors.toList()))
                .orElse(new ArrayList<>());

            // Create a map for fast lookup of log directories by broker ID
            Map<Integer, String> brokerToLogDir = replicaWithLogDirs.stream()
                .collect(Collectors.toMap(r -> r.id, r -> r.logDir));

            Partition p = new Partition();
            p.setTopic(topicPartition.topic());
            p.setPartition(topicPartition.partition());
            
            // Set replicas in the correct order
            p.setReplicas(correctReplicaOrder);
            
            // Set log dirs in the same order as replicas
            p.setLogDirs(
                correctReplicaOrder.stream()
                    .map(brokerToLogDir::get)
                    .collect(Collectors.toList())
            );
            
            partitions.add(p);
        }));
        assignment.setPartitions(partitions);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(assignment);

        return json;
    }

    static void execute(Options options, Admin adminClient) throws Exception {
        String outputFormat = options.output();
        
        if ("table".equals(outputFormat)) {
            System.out.println(buildTable(options, adminClient));
        } else if ("reassignment-json".equals(outputFormat)) {
            System.out.println(buildAssignment(options, adminClient));
        } else {
            throw new IllegalArgumentException("Invalid output format: " + outputFormat + ". Use 'reassignment-json' or 'table'.");
        }
    }
    
    static String buildTable(Options options, Admin adminClient) throws Exception {
        DescribeClusterResult cluster = adminClient.describeCluster();
        List<Integer> brokers = cluster.nodes().get().stream().map(Node::id).collect(Collectors.toList());
        
        List<BrokerDirInfo> brokerDirInfos = new ArrayList<>();
        
        DescribeLogDirsResult logDirs = adminClient.describeLogDirs(brokers);
        logDirs.allDescriptions().get().forEach((brokerId, logDirDescriptionMap) -> {
            logDirDescriptionMap.forEach((directoryName, logDirDescription) -> {
                if (!options.brokers().isEmpty() && !options.brokers().contains(brokerId)) {
                    return;
                }
                
                long totalSize = logDirDescription.replicaInfos().entrySet().stream()
                    .filter(entry -> options.topics().isEmpty() || options.topics().contains(entry.getKey().topic()))
                    .mapToLong(entry -> entry.getValue().size())
                    .sum();
                
                int partitionCount = (int) logDirDescription.replicaInfos().entrySet().stream()
                    .filter(entry -> options.topics().isEmpty() || options.topics().contains(entry.getKey().topic()))
                    .count();
                
                brokerDirInfos.add(new BrokerDirInfo(brokerId, directoryName, totalSize, partitionCount));
            });
        });
        
        brokerDirInfos.sort((a, b) -> {
            int brokerComparison = Integer.compare(a.brokerId, b.brokerId);
            if (brokerComparison != 0) return brokerComparison;
            return a.directory.compareTo(b.directory);
        });
        
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-10s | %-50s | %-18s | %-10s%n", "broker id", "directory", "size(in byte)", "partitions"));
        sb.append("-".repeat(98)).append("\n");
        
        for (BrokerDirInfo info : brokerDirInfos) {
            sb.append(String.format("%-10d | %-50s | %-18d | %-10d%n", info.brokerId, info.directory, info.size, info.partitionCount));
        }
        
        return sb.toString();
    }
    
    private static class BrokerDirInfo {
        final int brokerId;
        final String directory;
        final long size;
        final int partitionCount;
        
        BrokerDirInfo(int brokerId, String directory, long size, int partitionCount) {
            this.brokerId = brokerId;
            this.directory = directory;
            this.size = size;
            this.partitionCount = partitionCount;
        }
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
        private final OptionSpec<String> outputOpt;

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
            outputOpt = parser.accepts("output", "Output format: 'reassignment-json' for JSON assignment or 'table' for table format")
                    .withRequiredArg()
                    .describedAs("Output format")
                    .ofType(String.class)
                    .defaultsTo("reassignment-json");

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

        private String output() {
            return options.valueOf(outputOpt);
        }
    }
}