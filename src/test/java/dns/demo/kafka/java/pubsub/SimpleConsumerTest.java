package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.AbstractKafkaTest;
import dns.demo.kafka.domain.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static dns.demo.kafka.java.pubsub.SimpleConsumer.*;
import static dns.demo.kafka.java.streams.util.StreamUtils.INPUT_TOPIC_STREAM;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@EmbeddedKafka
class SimpleConsumerTest extends AbstractKafkaTest {
    private static final int NUM_RECORDS = 10;

    private String topic;
    private List<ProducerRecord<String, String>> records;

    @BeforeEach
    void setUp(EmbeddedKafkaBroker broker) {
        topic = INPUT_TOPIC_STREAM + "-" + UUID.randomUUID();
        broker.addTopics(topic);
        records = IntStream.range(0, NUM_RECORDS)
                .mapToObj(i -> new ProducerRecord<>(topic, "key" + i, "value" + i))
                .toList();
    }

    @Test
    void consume(EmbeddedKafkaBroker broker) {
        assertThat(produceRecords(records, broker)).hasSize(NUM_RECORDS);

        int consumedRecords = SimpleConsumer.consume(getConsumerProperties(broker.getBrokersAsString()), topic, NUM_RECORDS);
        assertThat(consumedRecords).isEqualTo(NUM_RECORDS);
    }

    @Test
    void consumeManualCommit(EmbeddedKafkaBroker broker) {
        assertThat(produceRecords(records, broker)).hasSize(NUM_RECORDS);

        int consumedRecords = SimpleConsumer.consume(getManualCommitConsumerProperties(broker.getBrokersAsString()), topic, NUM_RECORDS);
        assertThat(consumedRecords).isEqualTo(NUM_RECORDS);
    }

    @Test
    void consumeWithAvroDeserializer(EmbeddedKafkaBroker broker) {
        Set<Person> expectedEmployees = SimpleProducerTest.produceRecordsWithAvroSerializer(broker, topic)
                .stream().map(ProducerRecord::value).collect(toUnmodifiableSet());
        assertThat(expectedEmployees).isNotEmpty();

        Set<Person> actualEmployees = new HashSet<>(expectedEmployees.size());

        Consumer<ConsumerRecord<String, Person>> recordConsumer = record -> {
            assertThat(record.value()).isInstanceOf(Person.class);
            actualEmployees.add(record.value());
        };
        int consumedRecords = SimpleConsumer.consume(getConsumerPropertiesWithAvroDeserializer(broker.getBrokersAsString(), MOCK_SCHEMA_REGISTRY_URL),
                List.of(topic), expectedEmployees.size(), recordConsumer);
        assertThat(consumedRecords).isEqualTo(expectedEmployees.size());
        assertThat(actualEmployees).isEqualTo(expectedEmployees);
    }

    @Test
    void consumerLabExercise(EmbeddedKafkaBroker broker) throws IOException {
        List<String> topics = List.of("inventory_purchases-c", "apple_purchases-c");

        // Disable the creation of the topic as it seems to cause an error org.apache.kafka.common.errors.OutOfOrderSequenceException: Out of order sequence number for producer 1
        //broker.addTopics(topics.get(0), topics.get(1));

        List<ProducerRecord<String, String>> records = SimpleProducerTest.produceLabRecords(topics.get(0), topics.get(1), broker);
        Path tempFile = Files.createTempFile("output-data-", null);
        log.info("Created temp file[{}]", tempFile);

        Consumer<ConsumerRecord<String, String>> recordConsumer = record -> {
            try {
                String line = record.key() + record.value() + System.lineSeparator();
                Files.writeString(tempFile, line, StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        int consumedRecords = SimpleConsumer.consume(getManualCommitConsumerProperties(broker.getBrokersAsString()),
                topics, records.size(), recordConsumer);
        List<String> actualLines = Files.readAllLines(tempFile);
        assertThat(consumedRecords).isEqualTo(records.size());
        assertThat(actualLines).hasSize(records.size());
    }
}