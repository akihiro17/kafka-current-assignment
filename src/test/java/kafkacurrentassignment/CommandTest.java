package kafkacurrentassignment;

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class CommandTest {
    @Test
    @SuppressWarnings("unchecked")
    public void shouldBuildAssignment() {
        Node brokerOne = new Node(1, "localhost", 9092);
        Node brokerTwo = new Node(2, "localhost", 9093);

        Command.Options options = new Command.Options("");
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(2).controller(0).build()) {
            Collection<NewTopic> topics = new ArrayList<>();
            topics.add(new NewTopic("test", 1, (short) 2));
            adminClient.createTopics(topics);
/*
            String expected = """
{
  "version": 1,
  "partitions": [
    {
      "topic": "test",
      "partition": 0,
      "replicas": [
        0,
        1
      ],
      "log_dirs": [
        "/tmp/kafka-logs",
        "/tmp/kafka-logs"
      ]
    }
  ]
}""";
 */
            String expected = "{\n" +
                "  \"version\": 1,\n" +
                "  \"partitions\": [\n" +
                "    {\n" +
                "      \"topic\": \"test\",\n" +
                "      \"partition\": 0,\n" +
                "      \"replicas\": [\n" +
                "        0,\n" +
                "        1\n" +
                "      ],\n" +
                "      \"log_dirs\": [\n" +
                "        \"/tmp/kafka-logs\",\n" +
                "        \"/tmp/kafka-logs\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

            String got = Command.buildAssignment(options, adminClient);
            assertEquals(expected, got);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
