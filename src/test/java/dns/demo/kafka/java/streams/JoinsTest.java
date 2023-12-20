package dns.demo.kafka.java.streams;

import dns.demo.kafka.AbstractKafkaTest;
import dns.demo.kafka.java.streams.util.StreamUtils;
import dns.demo.kafka.java.streams.util.StreamUtils.StreamPair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.*;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.*;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EmbeddedKafka
class JoinsTest extends AbstractKafkaTest {

    private final Properties streamProperties = StreamUtils.getStreamProperties();
    private String leftTopic;
    private String rightTopic;
    private String innerJoinTopic;
    private String leftJoinTopic;
    private String outerJoinTopic;

    @BeforeEach
    void setUp(EmbeddedKafkaBroker broker) {
        leftTopic = "left-topic" + UUID.randomUUID();
        rightTopic = "right-topic" + UUID.randomUUID();
        innerJoinTopic = "inner-join-topic" + UUID.randomUUID();
        leftJoinTopic = "left-join-topic" + UUID.randomUUID();
        outerJoinTopic = "outer-join-topic" + UUID.randomUUID();

        broker.addTopics(leftTopic, rightTopic, innerJoinTopic, leftJoinTopic, outerJoinTopic);
        streamProperties.put(BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
    }

    @Test
    void joinStreams(EmbeddedKafkaBroker broker) {
        String k1 = "k1";
        String k2 = "k2";
        String k3 = "k3";
        String k4 = "k4";

        List<Map.Entry<String, Object>> leftRecords = List.of(Map.entry(k1, "Name1"), Map.entry(k2, "Name2"), Map.entry(k3, "Name3"));
        List<Map.Entry<String, Object>> rightRecords = List.of(Map.entry(k1, "Lastname1"), Map.entry(k2, "Lastname2"), Map.entry(k4, "Lastname4"));

        StreamPair pair = Joins.joinStreams(streamProperties, leftTopic, rightTopic, innerJoinTopic, leftJoinTopic, outerJoinTopic);

        try (Consumer<String, String> consumerInnerJoin = createConsumerAndSubscribe(innerJoinTopic, broker);
             Consumer<String, String> consumerLeftJoin = createConsumerAndSubscribe(leftJoinTopic, broker);
             Consumer<String, String> consumerOuterJoin = createConsumerAndSubscribe(outerJoinTopic, broker)) {

            produceRecords(leftRecords, leftTopic, broker);
            produceRecords(rightRecords, rightTopic, broker);

            TopologyTestDriver driver = new TopologyTestDriver(pair.topology(), streamProperties);

            Map<String, String> recordsMap = new HashMap<>();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                recordsMap.putAll(consumeRecords(innerJoinTopic, consumerInnerJoin));
                assertThat(recordsMap).hasSize(2);
                assertThat(recordsMap).contains(Map.entry(k1, "left=Name1, right=Lastname1"));
                assertThat(recordsMap).contains(Map.entry(k2, "left=Name2, right=Lastname2"));
            });


            driver.advanceWallClockTime(Duration.ofSeconds(30));
            recordsMap.clear();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                recordsMap.putAll(consumeRecords(leftJoinTopic, consumerLeftJoin));
                assertThat(recordsMap).hasSize(3);
                assertThat(recordsMap).contains(Map.entry(k3, "left=Name3, right=null"));
                assertThat(recordsMap).contains(Map.entry(k1, "left=Name1, right=Lastname1"));
                assertThat(recordsMap).contains(Map.entry(k2, "left=Name2, right=Lastname2"));
            });

            recordsMap.clear();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                recordsMap.putAll(consumeRecords(outerJoinTopic, consumerOuterJoin));
                assertThat(recordsMap).hasSize(4);
                assertThat(recordsMap).contains(Map.entry(k4, "left=null, right=Lastname4"));
                assertThat(recordsMap).contains(Map.entry(k3, "left=Name3, right=null"));
                assertThat(recordsMap).contains(Map.entry(k1, "left=Name1, right=Lastname1"));
                assertThat(recordsMap).contains(Map.entry(k2, "left=Name2, right=Lastname2"));
            });

            //pair.kafkaStreams().close();
        }
    }

    private Map<String, String> consumeRecords(String topic, Consumer<String, String> consumer) {
        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(100));
        return StreamSupport.stream(consumerRecords.records(topic).spliterator(), false)
                .collect(groupingBy(ConsumerRecord::key,
                        mapping(ConsumerRecord::value, reducing("", (val1, val2) -> val2))));
    }
}