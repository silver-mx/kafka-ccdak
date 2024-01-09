package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.AbstractKafkaTest;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static dns.demo.kafka.java.pubsub.SimpleConsumer.getConsumerProperties;
import static dns.demo.kafka.java.pubsub.SimpleConsumer.getManualCommitConsumerProperties;
import static dns.demo.kafka.java.streams.util.StreamUtils.INPUT_TOPIC_STREAM;
import static org.assertj.core.api.Assertions.assertThat;

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
}