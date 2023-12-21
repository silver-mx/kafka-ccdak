package dns.demo.kafka.java.streams;

import dns.demo.kafka.AbstractKafkaTest;
import dns.demo.kafka.java.streams.util.StreamUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.*;
import java.util.stream.StreamSupport;

import static dns.demo.kafka.java.streams.util.StreamUtils.INPUT_TOPIC_STREAM;
import static dns.demo.kafka.java.streams.util.StreamUtils.OUTPUT_TOPIC_STREAM_1;
import static java.util.stream.Collectors.*;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EmbeddedKafka
class AggregationsTest extends AbstractKafkaTest {

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
    void groupByKey(EmbeddedKafkaBroker broker) {
        String key1 = "key-1";
        String key2 = "key-2";
        Map<String, Object> extraConsumerIntProps = Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        Map<String, Object> extraConsumerLongProps = Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        List<ProducerRecord<String, String>> inputRecords = List.of(
                new ProducerRecord<>(inputTopic, key1, "1"),
                new ProducerRecord<>(inputTopic, key1, "2"),
                new ProducerRecord<>(inputTopic, key2, "1")
        );
        produceRecords(inputRecords, broker);

        String outputTopicCountChars = "count-chars-" + outputTopic;
        String outputTopicCountRecords = "count-records-" + outputTopic;
        String outputTopicReducedString = "reduce-string-" + outputTopic;

        try (KafkaStreams streams = Aggregations.groupByKey(streamProperties, inputTopic, outputTopicCountChars,
                outputTopicCountRecords, outputTopicReducedString);
             Consumer<String, Integer> consumerInt = createConsumerAndSubscribe(outputTopicCountChars, broker, extraConsumerIntProps);
             Consumer<String, Long> consumerLong = createConsumerAndSubscribe(outputTopicCountRecords, broker, extraConsumerLongProps);
             Consumer<String, String> consumerStr = createConsumerAndSubscribe(outputTopicReducedString, broker)) {

            streams.start();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                Map<String, Optional<Integer>> recordsCountCharsMap = getRecordsCountCharsMap(consumerInt, outputTopicCountChars);
                assertThat(recordsCountCharsMap.get(key1)).isEqualTo(Optional.of(2));
                assertThat(recordsCountCharsMap.get(key2)).isEqualTo(Optional.of(1));
            });

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                Map<String, Optional<Long>> recordsCountRecordsMap = getRecordsCountRecordsMap(consumerLong, outputTopicCountRecords);
                assertThat(recordsCountRecordsMap.get(key1)).isEqualTo(Optional.of(2L));
                assertThat(recordsCountRecordsMap.get(key2)).isEqualTo(Optional.of(1L));
            });

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                Map<String, List<String>> recordsReduceStringMap = getRecordsReducedStringMap(consumerStr, outputTopicReducedString);
                assertThat(recordsReduceStringMap.get(key1)).isEqualTo(List.of("1", "1-2"));
                assertThat(recordsReduceStringMap.get(key2)).isEqualTo(List.of("1"));
            });
        }
    }

    private static Map<String, Optional<Integer>> getRecordsCountCharsMap(Consumer<String, Integer> consumer, String topic) {
        ConsumerRecords<String, Integer> recordsCountChars = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(50));
        return StreamSupport.stream(recordsCountChars.spliterator(), false)
                .filter(record -> record.topic().equals(topic))
                .collect(
                        groupingBy(
                                ConsumerRecord::key,
                                mapping(ConsumerRecord::value, maxBy(Comparator.comparingInt(i -> i)))
                        )
                );
    }

    private static Map<String, Optional<Long>> getRecordsCountRecordsMap(Consumer<String, Long> consumer, String topic) {
        ConsumerRecords<String, Long> recordsCountRecords = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(50));
        return StreamSupport.stream(recordsCountRecords.spliterator(), false)
                .filter(record -> record.topic().equals(topic))
                .collect(
                        groupingBy(
                                ConsumerRecord::key,
                                mapping(ConsumerRecord::value, maxBy(Comparator.comparingLong(i -> i)))
                        )
                );
    }

    private static Map<String, List<String>> getRecordsReducedStringMap(Consumer<String, String> consumer, String topic) {
        ConsumerRecords<String, String> recordsReducedString = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(50));
        return StreamSupport.stream(recordsReducedString.spliterator(), false)
                .filter(record -> record.topic().equals(topic))
                .collect(
                        groupingBy(
                                ConsumerRecord::key,
                                mapping(ConsumerRecord::value, toList())
                        )
                );
    }
}