package dns.demo.kafka;

import dns.demo.kafka.java.pubsub.SimpleConsumer;
import dns.demo.kafka.java.pubsub.SimpleProducer;
import jakarta.annotation.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterEach;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static java.util.Objects.nonNull;

public class AbstractKafkaTest {

    protected KafkaStreams kafkaStreams;

    @AfterEach
    protected void closeKafkaStreams() {
        if (nonNull(kafkaStreams)) {
            kafkaStreams.close();
        }
    }

    public void produceRecords(int numRecords, String topic, EmbeddedKafkaBroker broker) {
        Map<String, Object> propsMap = SimpleProducer.getProducerProperties(broker.getBrokersAsString());
        SimpleProducer.produce(numRecords, propsMap, topic);
    }

    public int consumeRecords(String topic, EmbeddedKafkaBroker broker, @Nullable Integer expectedRecords) {
        Consumer<String, String> consumer = createConsumerAndSubscribe(topic, broker);

        if (nonNull(expectedRecords)) {
            return KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1), expectedRecords).count();
        }

        return KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1)).count();
    }

    public Consumer<String, String> createConsumerAndSubscribe(String topic, EmbeddedKafkaBroker broker) {
        return createConsumerAndSubscribe(List.of(topic), broker);
    }

    public Consumer<String, String> createConsumerAndSubscribe(List<String> topics, EmbeddedKafkaBroker broker) {
        Map<String, Object> propsMap = SimpleConsumer.getConsumerProperties(broker.getBrokersAsString());
        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(propsMap);
        Consumer<String, String> consumer = factory.createConsumer();
        consumer.subscribe(topics);

        return consumer;
    }
}
