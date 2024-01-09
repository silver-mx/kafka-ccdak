package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.AbstractKafkaTest;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.List;
import java.util.UUID;

import static dns.demo.kafka.java.streams.util.StreamUtils.INPUT_TOPIC_STREAM;
import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka
class SimpleProducerTest extends AbstractKafkaTest {

    private String topic;

    @BeforeEach
    void setUp(EmbeddedKafkaBroker broker) {
        topic = INPUT_TOPIC_STREAM + "-" + UUID.randomUUID();
        broker.addTopics(topic);
    }

    @Test
    void produce(EmbeddedKafkaBroker broker) {
        int producedRecords = 5;
        List<RecordMetadata> recordMetadata = SimpleProducer.produce(
                producedRecords, SimpleProducer.getProducerExtendedProperties(broker.getBrokersAsString()), topic);
        assertThat(recordMetadata).hasSize(producedRecords);

        int consumedRecords = consumeRecords(topic, broker, producedRecords);
        assertThat(consumedRecords).isEqualTo(producedRecords);
    }

    @Test
    void produceAdvanced(EmbeddedKafkaBroker broker) {
        int producedRecords = 10;
        List<RecordMetadata> recordMetadata = SimpleProducer.produceAdvanced(
                producedRecords, SimpleProducer.getProducerExtendedProperties(broker.getBrokersAsString()), topic);
        assertThat(recordMetadata).hasSize(producedRecords);

        int consumedRecords = consumeRecords(topic, broker, producedRecords);
        assertThat(consumedRecords).isEqualTo(producedRecords);
    }
}