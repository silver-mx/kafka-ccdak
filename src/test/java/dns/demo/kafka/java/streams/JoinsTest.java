package dns.demo.kafka.java.streams;

import dns.demo.kafka.AbstractKafkaTest;
import dns.demo.kafka.java.streams.util.StreamUtils;
import dns.demo.kafka.java.streams.util.StreamUtils.StreamPair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

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

        broker.getTopics();

        List<Map.Entry<String, String>> leftRecords = List.of(Map.entry(k1, "Name1"), Map.entry(k2, "Name2"), Map.entry(k3, "Name3"));
        List<Map.Entry<String, String>> rightRecords = List.of(Map.entry(k1, "Lastname1"), Map.entry(k2, "Lastname2"), Map.entry(k4, "Lastname4"));

        StreamPair pair = Joins.joinStreams(streamProperties, leftTopic, rightTopic, innerJoinTopic, leftJoinTopic, outerJoinTopic);

        try (KafkaStreams streams = pair.kafkaStreams();
             TopologyTestDriver testDriver = createTopologyTestDriver(pair.topology(), Serdes.StringSerde.class, Serdes.StringSerde.class);
             Serdes.StringSerde stringSerde = new Serdes.StringSerde()) {

            TestInputTopic<String, String> leftInputTopic = testDriver.createInputTopic(leftTopic, stringSerde.serializer(), stringSerde.serializer());
            TestInputTopic<String, String> rightInputTopic = testDriver.createInputTopic(rightTopic, stringSerde.serializer(), stringSerde.serializer());
            leftRecords.forEach(record -> leftInputTopic.pipeInput(record.getKey(), record.getValue(), Instant.now()));
            rightRecords.forEach(record -> rightInputTopic.pipeInput(record.getKey(), record.getValue(), Instant.now()));

            // Start stream
            streams.start();

            Map<String, String> recordsMap = new HashMap<>();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                recordsMap.putAll(consumeRecords(innerJoinTopic, testDriver));
                assertThat(recordsMap).hasSize(2);
                assertThat(recordsMap).contains(Map.entry(k1, "left=Name1, right=Lastname1"));
                assertThat(recordsMap).contains(Map.entry(k2, "left=Name2, right=Lastname2"));
            });

            recordsMap.clear();
            /* NOTE: It is required to produce a record with time in the future to advance the stream time and let it know that
             * the join window (1 second) has been closed. This is required for the leftJoin and outerJoin functions to
             * publish null values when one of the join sides has missing values. This is one effect caused by
             * JoinWindows.ofTimeDifferenceWithNoGrace.
             *  */
            leftInputTopic.pipeInput("randomKey", "randomValue", Instant.now().plusSeconds(5));


            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                recordsMap.putAll(consumeRecords(leftJoinTopic, testDriver));
                assertThat(recordsMap).hasSize(3);
                assertThat(recordsMap).contains(Map.entry(k1, "left=Name1, right=Lastname1"));
                assertThat(recordsMap).contains(Map.entry(k2, "left=Name2, right=Lastname2"));
                assertThat(recordsMap).contains(Map.entry(k3, "left=Name3, right=null"));
            });

            recordsMap.clear();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                recordsMap.putAll(consumeRecords(outerJoinTopic, testDriver));
                assertThat(recordsMap).hasSize(4);
                assertThat(recordsMap).contains(Map.entry(k4, "left=null, right=Lastname4"));
                assertThat(recordsMap).contains(Map.entry(k3, "left=Name3, right=null"));
                assertThat(recordsMap).contains(Map.entry(k1, "left=Name1, right=Lastname1"));
                assertThat(recordsMap).contains(Map.entry(k2, "left=Name2, right=Lastname2"));
            });
        }
    }

    private Map<String, String> consumeRecords(String topic, TopologyTestDriver testDriver) {
        try (Serdes.StringSerde stringSerde = new Serdes.StringSerde()) {
            TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(topic, stringSerde.deserializer(), stringSerde.deserializer());
            List<TestRecord<String, String>> records = outputTopic.readRecordsToList();
            return records.stream().collect(groupingBy(TestRecord::key,
                    mapping(TestRecord::value, reducing("", (val1, val2) -> val2))));
        }
    }
}