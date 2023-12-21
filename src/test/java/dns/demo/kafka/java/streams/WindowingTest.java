package dns.demo.kafka.java.streams;

import dns.demo.kafka.AbstractKafkaTest;
import dns.demo.kafka.java.streams.util.StreamUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.*;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(SpringExtension.class)
@EmbeddedKafka
class WindowingTest extends AbstractKafkaTest {
    private final Properties streamProperties = StreamUtils.getStreamProperties();
    private String inputTopic;
    private String outputTopic;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @BeforeEach
    void setUp() {
        inputTopic = "inventory_purchases-" + UUID.randomUUID();
        outputTopic = "total_purchases-" + UUID.randomUUID();

        broker.addTopics(inputTopic, outputTopic);
        streamProperties.put(BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
    }

    private static Stream<Arguments> windowedSource() {
        Duration windowSize = Duration.ofSeconds(1);
        Duration advanceSize = Duration.ofMillis(500);

        return Stream.of(Arguments.of("HoppingWindow", TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceSize)),
                Arguments.of("TumblingWindow", TimeWindows.ofSizeWithNoGrace(windowSize)));
    }

    @ParameterizedTest
    @MethodSource("windowedSource")
    void groupByWindow(String description, TimeWindows window) {
        String lemonKey = "lemon";
        String orangeKey = "orange";

        Map<String, Object> extraConsumerProps = Map.of(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, WindowedSerdes.timeWindowedSerdeFrom(String.class, 1000).deserializer().getClass().getName(),
                StreamsConfig.WINDOW_SIZE_MS_CONFIG, 1000L,
                StreamsConfig.WINDOWED_INNER_CLASS_SERDE, Serdes.StringSerde.class.getName()
        );

        List<ProducerRecord<String, String>> inputRecords = List.of(
                new ProducerRecord<>(inputTopic, lemonKey, "a"),
                new ProducerRecord<>(inputTopic, orangeKey, "a"),
                new ProducerRecord<>(inputTopic, lemonKey, "b")
        );
        produceRecords(inputRecords, broker);

        try (KafkaStreams streams = Windowing.groupByWindow(streamProperties, inputTopic, outputTopic, window);
             Consumer<Windowed<String>, String> consumer = createConsumerAndSubscribe(outputTopic, broker, extraConsumerProps)) {

            streams.start();

            Map<String, String> recordsMap = new HashMap<>();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                recordsMap.putAll(getRecordsMap(consumer));
                assertThat(recordsMap.get(lemonKey)).isEqualTo("ab");
                assertThat(recordsMap.get(orangeKey)).isEqualTo("a");
            });
        }
    }

    private Map<String, String> getRecordsMap(Consumer<Windowed<String>, String> consumer) {
        ConsumerRecords<Windowed<String>, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(100));
        return StreamSupport.stream(records.spliterator(), false)
                .collect(
                        groupingBy(key -> key.key().key(),
                                mapping(ConsumerRecord::value,
                                        reducing("", (v1, v2) -> v2)))
                );

    }
}