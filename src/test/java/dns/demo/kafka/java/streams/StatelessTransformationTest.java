package dns.demo.kafka.java.streams;

import dns.demo.kafka.AbstractKafkaTest;
import dns.demo.kafka.java.streams.util.StreamUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.Properties;
import java.util.UUID;

import static dns.demo.kafka.java.streams.util.StreamUtils.*;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@EmbeddedKafka
class StatelessTransformationTest extends AbstractKafkaTest {

    private final Properties streamProperties = StreamUtils.getStreamProperties();
    private String inputTopic;
    private String outputTopic1;
    private String outputTopic2;

    @BeforeEach
    void setUp(EmbeddedKafkaBroker broker) {
        inputTopic = INPUT_TOPIC_STREAM + "-" + UUID.randomUUID();
        outputTopic1 = OUTPUT_TOPIC_STREAM_1 + "-" + UUID.randomUUID();
        outputTopic2 = OUTPUT_TOPIC_STREAM_2 + "-" + UUID.randomUUID();
        broker.addTopics(inputTopic, outputTopic1, outputTopic2);
        streamProperties.put(BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
    }

    @Test
    void splitStreamIntoTwoStreams(EmbeddedKafkaBroker broker) {
        int expectedRecords = 100;

        produceRecords(expectedRecords, inputTopic, broker);
        StatelessTransformation.splitStreamIntoTwoStreams(streamProperties, inputTopic, outputTopic1, outputTopic2);
        int recordCount1 = consumeRecords(outputTopic1, broker);
        int recordCount2 = consumeRecords(outputTopic2, broker);

        assertAll(
                () -> assertThat(recordCount1).isEqualTo(12),
                () -> assertThat(recordCount2).isEqualTo(88)
        );
    }
}