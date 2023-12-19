package dns.demo.kafka.java.streams;

import dns.demo.kafka.AbstractKafkaTest;
import dns.demo.kafka.java.pubsub.SimpleConsumer;
import dns.demo.kafka.java.pubsub.SimpleProducer;
import dns.demo.kafka.java.streams.util.StreamUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static dns.demo.kafka.java.streams.util.StreamUtils.*;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.*;

@EmbeddedKafka
class CopyStreamTest extends AbstractKafkaTest {

    private final Properties streamProperties = StreamUtils.getStreamProperties();
    private String inputTopic;
    private String outputTopic;

    @BeforeEach
    void setUp(EmbeddedKafkaBroker broker) {
        inputTopic = INPUT_TOPIC_STREAM + "-" + UUID.randomUUID();
        outputTopic = OUTPUT_TOPIC_STREAM_1 + "-" + UUID.randomUUID();
        broker.addTopics(inputTopic, outputTopic);
        streamProperties.put(BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
    }

    @Test
    void copyDataFromTopicToTopic(EmbeddedKafkaBroker broker) {
        int expectedRecords = 10;

        produceRecords(expectedRecords, inputTopic, broker);
        kafkaStreams = CopyStream.copyDataFromTopicToTopic(streamProperties, inputTopic, outputTopic);

        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            int recordCount = 0;
            try (Consumer<String, Object> consumer = createConsumerAndSubscribe(outputTopic, broker)) {
                while (recordCount != expectedRecords) {
                    recordCount += KafkaTestUtils.getRecords(consumer, Duration.ofMillis(100)).count();
                }
            }
        });
    }
}