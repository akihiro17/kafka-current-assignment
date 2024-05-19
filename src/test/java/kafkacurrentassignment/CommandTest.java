package kafkacurrentassignment;

import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.util.*;

import static kafkacurrentassignment.Command.execute;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class CommandTest {
    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotThrowWhenDuplicatedBrokers() {
        Node brokerOne = new Node(1, "localhost", 9092);
        Node brokerTwo = new Node(2, "localhost", 9093);


        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(2).controller(0).build()) {
            Collection<NewTopic> topics = new ArrayList<>();
            topics.add(new NewTopic("test", 1, (short) 2));
            adminClient.createTopics(topics);

            System.out.println(adminClient.listTopics().names());

            Command.execute(adminClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
