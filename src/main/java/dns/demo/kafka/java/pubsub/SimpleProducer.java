package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.domain.Person;
import dns.demo.kafka.domain.Purchase;
import dns.demo.kafka.util.ClusterUtils;
import dns.demo.kafka.util.MiscUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static dns.demo.kafka.java.ksql.QueryStream.*;
import static dns.demo.kafka.util.ClusterUtils.*;
import static java.util.Objects.nonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
public class SimpleProducer {

    public static void produce(int numRecords, String topic) {
        produce(numRecords, getProducerExtendedProperties(getBroker()), topic);
    }

    public static List<RecordMetadata> produce(int numRecords, Map<String, Object> props, String topic) {
        List<ProducerRecord<String, String>> records = IntStream.range(1, numRecords + 1)
                .mapToObj(i -> new ProducerRecord<>(topic, "key-" + i, "message-value-" + i))
                .toList();

        return produce(records, props);
    }

    public static <K, V> List<RecordMetadata> produce(List<ProducerRecord<K, V>> records, Map<String, Object> props) {
        Supplier<List<Future<RecordMetadata>>> producerLogic = () -> {
            List<Future<RecordMetadata>> futureList = new ArrayList<>(records.size());

            try (KafkaProducer<K, V> producer = new KafkaProducer<>(props)) {
                records.forEach(record -> {
                    log.info("Publishing message: key={} value={} topic={}", record.key(), record.value(), record.topic());
                    futureList.add(producer.send(record));
                });
            }

            return futureList;
        };

        return produce(producerLogic);
    }

    private static List<RecordMetadata> produce(Supplier<List<Future<RecordMetadata>>> producerLogic) {
        List<Future<RecordMetadata>> futureList = producerLogic.get();
        return futureList.stream().map(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }).toList();
    }

    public static List<RecordMetadata> produceSelectPartition(int numRecords, Map<String, Object> props, String topic) {
        Supplier<List<Future<RecordMetadata>>> producerLogic = () -> {
            List<Future<RecordMetadata>> futureList = new ArrayList<>(numRecords);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                IntStream.range(1, numRecords + 1).forEach(i -> {
                    int partition = i % 2 == 0 ? 0 : 1;
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, "key-" + i, "message-value-" + i);
                    Callback callback = (metadata, exception) -> {

                        if (nonNull(exception)) {
                            log.error("Error publishing message: {}", exception.getMessage());
                        } else {
                            log.info("Published message: key={} value={} topic={} partition={} offset={}", record.key(),
                                    record.value(), metadata.topic(), metadata.partition(), metadata.offset());
                        }
                    };
                    futureList.add(producer.send(record, callback));
                });
            }

            return futureList;
        };

        return produce(producerLogic);
    }

    public static Map<String, Object> getProducerProperties(String broker) {
        return Map.of(
                //CLIENT_ID_CONFIG, "spring-boot-producer",
                BOOTSTRAP_SERVERS_CONFIG, broker,
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),

                // PRODUCER TUNING

                /* Using retries=0 causes the tests to fail with NotLeaderOrFollowerException, maybe because som writes
                 fail and cannot be retried.
                 */
                RETRIES_CONFIG, 3,
                ACKS_CONFIG, "all",
                ENABLE_IDEMPOTENCE_CONFIG, true, // Default
                /* Allowing retries while setting enable.idempotence to false and max.in.flight.requests.per.connection
                 to greater than 1 will potentially change the ordering of records because if two batches are sent to a
                 single partition, and the first fails and is retried but the second succeeds, then the records in the
                 second batch may appear first.*/
                MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1, // Default (5).
                BATCH_SIZE_CONFIG, 65536 // Default 16384
        );
    }

    public static Map<String, Object> getProducerExtendedProperties(String broker) {
        Map<String, Object> producerProperties = new HashMap<>(getProducerProperties(broker));
        producerProperties.putAll(Map.of(
                BUFFER_MEMORY_CONFIG, "12582912",
                CONNECTIONS_MAX_IDLE_MS_CONFIG, "300000"));

        return Collections.unmodifiableMap(producerProperties);
    }

    public static Map<String, Object> getProducerPropertiesWithTls(String broker) throws IOException {
        Map<String, Object> props = new HashMap<>(getProducerExtendedProperties(broker));
        return MiscUtils.addTlsConfigurationProperties(props);
    }

    public static Map<String, Object> getProducerPropertiesWithAvroSerializer(String broker, String schemaRegistryUrl) {
        Map<String, Object> props = new HashMap<>(getProducerExtendedProperties(broker));
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        /* Best practice is to disable auto.register.schemas in production
        (see https://docs.confluent.io/platform/current/schema-registry/schema_registry_onprem_tutorial.html#auto-schema-registration)*/
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        return Collections.unmodifiableMap(props);
    }

    public static Map<String, Object> getProducerPropertiesWithTlsAndAvroSerializer(String broker, String schemaRegistryUrl) throws IOException {
        Map<String, Object> props = new HashMap<>(getProducerPropertiesWithTls(broker));
        props.putAll(new HashMap<>(getProducerPropertiesWithAvroSerializer(broker, schemaRegistryUrl)));

        return Collections.unmodifiableMap(props);
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            produce(100, "inventory");
        } else if (args[0].equals("--with-callback")) {
            produceSelectPartition(100, getProducerExtendedProperties(getBroker()), "inventory");
        } else if (args[0].equals("--with-avro")) {
            List<ProducerRecord<String, Person>> producerRecords = getAvroProducerPersonRecords("employees");
            produce(producerRecords, getProducerPropertiesWithAvroSerializer(getBroker(), getSchemaRegistryUrl()));
        } else if (args[0].equals("--with-avro-lab")) {
            List<ProducerRecord<String, Purchase>> producerRecords = getAvroProducerPurchaseRecords("purchases");
            produce(producerRecords, getProducerPropertiesWithAvroSerializer(getBroker(), getSchemaRegistryUrl()));
        } else if (args[0].equals("--with-tls-avro")) {
            List<ProducerRecord<String, Person>> producerRecords = getAvroProducerPersonRecords("employees-tls");
            produce(producerRecords, getProducerPropertiesWithTlsAndAvroSerializer(getBrokerTls(), getSchemaRegistryUrl()));
        } else if (args[0].equals("--with-tls")) {
            produce(100, getProducerPropertiesWithTls(getBrokerTls()), "inventory-tls")
                    .forEach(metadata ->
                            log.info("Published message: topic={} partition={} offset={}",
                                    metadata.topic(), metadata.partition(), metadata.offset())
                    );
        } else if (args[0].equals("--for-ksql-demo")) {
            ClusterUtils.deleteAndCreateTopics(List.of(MEMBER_SIGNUPS_TOPIC, MEMBER_CONTACT_TOPIC));
            List<ProducerRecord<String, String>> signupRecords = IntStream.range(0, EXPECTED_RECORDS)
                    .mapToObj(i -> {
                        String value = String.format("last-%d,name-%d,%b", i, i, (i % 2 == 0));
                        return new ProducerRecord<>(MEMBER_SIGNUPS_TOPIC, String.valueOf(i), value);
                    }).toList();
            produce(signupRecords, getProducerExtendedProperties(getBroker()));

            List<ProducerRecord<String, String>> contactRecords = IntStream.range(0, EXPECTED_RECORDS)
                    .mapToObj(i -> {
                        String value = String.format("name.lastname-new-%d@email.com", i);
                        return new ProducerRecord<>(MEMBER_CONTACT_TOPIC, String.valueOf(i), value);
                    }).toList();
            produce(contactRecords, getProducerExtendedProperties(getBroker()));
        }
    }

    public static List<ProducerRecord<String, Person>> getAvroProducerPersonRecords(String topic) {
        return Stream.of(
                        new Person(125745, "Kenny", "Armstrong", "kenny@linuxacademy.com", "@kenny"),
                        new Person(943256, "Terry", "Cox", "terry@linuxacademy.com", "@terry")
                )
                .map(person -> new ProducerRecord<>(topic, String.valueOf(person.getId()), person))
                .toList();
    }

    public static List<ProducerRecord<String, Purchase>> getAvroProducerPurchaseRecords(String topic) {
        return Stream.of(
                        new Purchase(1, "apples", 17, 123),
                        new Purchase(2, "oranges", 5, 456)
                )
                .map(person -> new ProducerRecord<>(topic, String.valueOf(person.getId()), person))
                .toList();
    }
}
