package dns.demo.kafka;

import dns.demo.kafka.java.pubsub.SimpleConsumer;
import dns.demo.kafka.java.pubsub.SimpleProducer;
import jakarta.annotation.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static java.util.Objects.nonNull;

public class AbstractKafkaTest {

    public List<RecordMetadata> produceRecords(int numRecords, String topic, EmbeddedKafkaBroker broker) {
        Map<String, Object> propsMap = SimpleProducer.getProducerProperties(broker.getBrokersAsString());
        return SimpleProducer.produce(numRecords, propsMap, topic);
    }

    public List<RecordMetadata> produceRecords(List<Map.Entry<String, Object>> records, String topic, EmbeddedKafkaBroker broker) {
        Map<String, Object> propsMap = SimpleProducer.getProducerProperties(broker.getBrokersAsString());
        DefaultKafkaProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(propsMap);
        List<Future<RecordMetadata>> futures = new ArrayList<>(records.size());

        try (Producer<String, Object> producer = producerFactory.createProducer()) {
            records.forEach(entry -> futures.add(producer.send(new ProducerRecord<>(topic, entry.getKey(), entry.getValue()))));
        }

        return futures.stream().map(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }).toList();
    }

    public int consumeRecords(String topic, EmbeddedKafkaBroker broker, @Nullable Integer expectedRecords) {
        Consumer<String, Object> consumer = createConsumerAndSubscribe(topic, broker);

        if (nonNull(expectedRecords)) {
            return KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1), expectedRecords).count();
        }

        return KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1)).count();
    }

    public <K, V> Consumer<K, V> createConsumerAndSubscribe(String topic, EmbeddedKafkaBroker broker) {
        return createConsumerAndSubscribe(List.of(topic), broker, Collections.emptyMap());
    }

    public <K, V> Consumer<K, V> createConsumerAndSubscribe(List<String> topics, EmbeddedKafkaBroker broker) {
        return createConsumerAndSubscribe(topics, broker, Collections.emptyMap());
    }

    public <K, V> Consumer<K, V> createConsumerAndSubscribe(String topic, EmbeddedKafkaBroker broker, Map<String, Object> extraPropsMap) {
        return createConsumerAndSubscribe(List.of(topic), broker, extraPropsMap);
    }

    public <K, V> Consumer<K, V> createConsumerAndSubscribe(List<String> topics, EmbeddedKafkaBroker broker, Map<String, Object> extraPropsMap) {
        Map<String, Object> propsMap = SimpleConsumer.getConsumerProperties(broker.getBrokersAsString());
        propsMap.putAll(extraPropsMap);
        DefaultKafkaConsumerFactory<K, V> factory = new DefaultKafkaConsumerFactory<>(propsMap);
        Consumer<K, V> consumer = factory.createConsumer();
        consumer.subscribe(topics);

        return consumer;
    }
}
