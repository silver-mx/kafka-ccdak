package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.AbstractKafkaTest;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static dns.demo.kafka.java.streams.util.StreamUtils.INPUT_TOPIC_STREAM;
import static java.util.Objects.requireNonNull;
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
    void produceSelectPartition(EmbeddedKafkaBroker broker) {
        int producedRecords = 10;
        List<RecordMetadata> recordMetadata = SimpleProducer.produceSelectPartition(
                producedRecords, SimpleProducer.getProducerExtendedProperties(broker.getBrokersAsString()), topic);
        assertThat(recordMetadata).hasSize(producedRecords);

        int consumedRecords = consumeRecords(topic, broker, producedRecords);
        assertThat(consumedRecords).isEqualTo(producedRecords);
    }

    @Test
    void produceLabExercise(EmbeddedKafkaBroker broker) throws IOException {
        String inventoryPurchases = "inventory_purchases";
        String applePurchases = "apple_purchases";
        broker.addTopics(inventoryPurchases, applePurchases);

        File file = new File(requireNonNull(this.getClass().getClassLoader().getResource("lab_resources/sample_transaction_log.txt")).getPath());
        Path filePath = Paths.get(file.getAbsolutePath());
        List<String> lines = Files.readAllLines(filePath);

        List<ProducerRecord<String, String>> records = lines.stream()
                .<ProducerRecord<String, String>>mapMulti((line, consumer) -> {
                    String[] lineArray = line.split(":");
                    String key = lineArray[0];
                    String value = lineArray[1];
                    consumer.accept(new ProducerRecord<>(inventoryPurchases, key, value));

                    if (key.equals("apples")) {
                        consumer.accept(new ProducerRecord<>(applePurchases, key, value));
                    }
                })
                .toList();

        List<RecordMetadata> recordMetadata = SimpleProducer.produce(records, SimpleProducer.getProducerExtendedProperties(broker.getBrokersAsString()));
        assertThat(recordMetadata).hasSize(records.size());

        int consumedRecords = consumeRecords(List.of(inventoryPurchases, applePurchases), broker, null);
        assertThat(consumedRecords).isEqualTo(records.size());
    }


}