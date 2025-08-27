package kafkacurrentassignment;

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonArray;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class CommandTest {
    @Test
    @SuppressWarnings("unchecked")
    public void shouldBuildAssignment() {
        Command.Options options = new Command.Options("");
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(100).controller(0).build()) {
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
