package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.AbstractKafkaTest;
import dns.demo.kafka.domain.Person;
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
import java.util.List;
import java.util.UUID;

import static dns.demo.kafka.java.pubsub.SimpleProducer.getProducerExtendedProperties;
import static dns.demo.kafka.java.pubsub.SimpleProducer.getProducerPropertiesWithAvroSerializer;
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
                producedRecords, getProducerExtendedProperties(broker.getBrokersAsString()), topic);
        assertThat(recordMetadata).hasSize(producedRecords);

        int consumedRecords = consumeRecords(topic, broker, producedRecords);
        assertThat(consumedRecords).isEqualTo(producedRecords);
    }

    @Test
    void produceSelectPartition(EmbeddedKafkaBroker broker) {
        int producedRecords = 10;
        List<RecordMetadata> recordMetadata = SimpleProducer.produceSelectPartition(
                producedRecords, getProducerExtendedProperties(broker.getBrokersAsString()), topic);
        assertThat(recordMetadata).hasSize(producedRecords);

        int consumedRecords = consumeRecords(topic, broker, producedRecords);
        assertThat(consumedRecords).isEqualTo(producedRecords);
    }

    @Test
    void produceWithAvroSerializer(EmbeddedKafkaBroker broker) {
        List<ProducerRecord<String, Person>> avroProducerRecords = produceRecordsWithAvroSerializer(broker, topic);

        int consumedRecords = consumeRecords(topic, broker, avroProducerRecords.size());
        assertThat(consumedRecords).isEqualTo(avroProducerRecords.size());
    }

    static List<ProducerRecord<String, Person>> produceRecordsWithAvroSerializer(EmbeddedKafkaBroker broker, String topic) {
        List<ProducerRecord<String, Person>> avroProducerRecords = SimpleProducer.getAvroProducerPersonRecords(topic);
        List<RecordMetadata> recordMetadata = SimpleProducer.produce(avroProducerRecords,
                getProducerPropertiesWithAvroSerializer(broker.getBrokersAsString(), MOCK_SCHEMA_REGISTRY_URL));
        assertThat(recordMetadata).hasSize(avroProducerRecords.size());

        return avroProducerRecords;
    }

    @Test
    void produceLabExercise(EmbeddedKafkaBroker broker) throws IOException {
        String inventoryPurchases = "inventory_purchases-p";
        String applePurchases = "apple_purchases-p";
        // Disable the creation of the topic as it seems to cause an error org.apache.kafka.common.errors.OutOfOrderSequenceException: Out of order sequence number for producer 1
        //broker.addTopics(inventoryPurchases, applePurchases);

        List<ProducerRecord<String, String>> records = produceLabRecords(inventoryPurchases, applePurchases, broker);

        int consumedRecords = consumeRecords(List.of(inventoryPurchases, applePurchases), broker, null);
        assertThat(consumedRecords).isEqualTo(records.size());
    }

    static List<ProducerRecord<String, String>> produceLabRecords(String inventoryPurchases, String applePurchases, EmbeddedKafkaBroker broker) throws IOException {
        File file = new File(requireNonNull(SimpleProducerTest.class.getClassLoader().getResource("lab_resources/sample_transaction_log.txt")).getPath());
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

        List<RecordMetadata> recordMetadata = SimpleProducer.produce(records, getProducerExtendedProperties(broker.getBrokersAsString()));
        assertThat(recordMetadata).hasSize(records.size());

        return records;
    }


}