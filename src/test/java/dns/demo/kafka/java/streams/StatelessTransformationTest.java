package dns.demo.kafka.java.streams;

import dns.demo.kafka.AbstractKafkaTest;
import dns.demo.kafka.java.streams.util.StreamUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static dns.demo.kafka.java.streams.util.StreamUtils.*;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

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
        int expectedRecords = 10;

        produceRecords(expectedRecords, inputTopic, broker);
        kafkaStreams = StatelessTransformation.splitStreamIntoTwoStreams(streamProperties, inputTopic, outputTopic1, outputTopic2);

        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            int recordCount1 = 0;
            int recordCount2 = 0;
            try (Consumer<String, String> consumer = createConsumerAndSubscribe(List.of(outputTopic1, outputTopic2), broker)) {
                while (recordCount1 != 2 && recordCount2 != 8) {
                    ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(100));
                    Function<String, Integer> getCount = (partition) -> (int) StreamSupport.stream(records.records(partition).spliterator(), false).count();
                    recordCount1 += getCount.apply(outputTopic1);
                    recordCount2 += getCount.apply(outputTopic2);;
                }
            }
        });
    }

    @Test
    void filterStream(EmbeddedKafkaBroker broker) {
        int expectedRecords = 10;

        produceRecords(expectedRecords, inputTopic, broker);
        kafkaStreams = StatelessTransformation.filterStream(streamProperties, inputTopic, outputTopic1);

        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            int recordCount = 0;
            try (Consumer<String, String> consumer = createConsumerAndSubscribe(outputTopic1, broker)) {
                while (recordCount != 2) {
                    recordCount += KafkaTestUtils.getRecords(consumer, Duration.ofMillis(100)).count();
                }
            }
        });
    }
}