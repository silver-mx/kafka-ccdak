package dns.demo.kafka.java.streams;

import dns.demo.kafka.AbstractKafkaTest;
import dns.demo.kafka.java.streams.util.StreamUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.*;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EmbeddedKafka
class CombinedStreamOperationsTest extends AbstractKafkaTest {
    private final Properties streamProperties = StreamUtils.getStreamProperties();
    private String inputTopic;
    private String outputTopic;

    @BeforeEach
    void setUp(EmbeddedKafkaBroker broker) {
        inputTopic = "inventory_purchases";
        outputTopic = "total_purchases";

        broker.addTopics(inputTopic, outputTopic);
        streamProperties.put(BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
    }

    @Test
    void countTotalPurchases(EmbeddedKafkaBroker broker) {
        String lemonKey = "lemon";
        String orangeKey = "orange";

        Map<String, Object> extraConsumerIntProps = Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

        List<ProducerRecord<String, String>> inputRecords = List.of(
                new ProducerRecord<>(inputTopic, lemonKey, "100"),
                new ProducerRecord<>(inputTopic, orangeKey, "24"),
                new ProducerRecord<>(inputTopic, lemonKey, "21")
        );
        produceRecords(inputRecords, broker);

        try (KafkaStreams streams = CombinedStreamOperations.countTotalPurchases(streamProperties, inputTopic, outputTopic);
             Consumer<String, Integer> consumer = createConsumerAndSubscribe(outputTopic, broker, extraConsumerIntProps)) {

            streams.start();

            Map<String, Integer> recordsMap = new HashMap<>();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                recordsMap.putAll(getRecordsMap(consumer));
                assertThat(recordsMap.get(lemonKey)).isEqualTo(121);
                assertThat(recordsMap.get(orangeKey)).isEqualTo(24);
            });
        }
    }

    private Map<String, Integer> getRecordsMap(Consumer<String, Integer> consumer) {
        ConsumerRecords<String, Integer> records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(100));
        return StreamSupport.stream(records.spliterator(), false)
                .collect(
                        groupingBy(ConsumerRecord::key,
                                mapping(ConsumerRecord::value,
                                        reducing(0, (v1, v2) -> v2)))
                );

    }
}