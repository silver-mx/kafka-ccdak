package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.util.ClusterUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.Objects.nonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
public class SimpleProducer {

    public static void produce(int numRecords, String topic) {
        produce(numRecords, getProducerExtendedProperties(ClusterUtils.getBroker()), topic);
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
                records.forEach(record -> futureList.add(producer.send(record)));
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
                BOOTSTRAP_SERVERS_CONFIG, broker,
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public static Map<String, Object> getProducerExtendedProperties(String broker) {
        Map<String, Object> producerProperties = new HashMap<>(getProducerProperties(broker));
        producerProperties.putAll(Map.of(
                ACKS_CONFIG, "all",
                BUFFER_MEMORY_CONFIG, "12582912",
                CONNECTIONS_MAX_IDLE_MS_CONFIG, "300000"));

        return Collections.unmodifiableMap(producerProperties);
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            SimpleProducer.produce(100, "inventory");
        } else if (args[0].equals("--with-callback")) {
            SimpleProducer.produceSelectPartition(100, getProducerExtendedProperties(ClusterUtils.getBroker()), "inventory");
        }
    }
}
