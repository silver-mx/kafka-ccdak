package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.util.ClusterUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class SimpleProducer {

    public static void produce(int numRecords, String topic) {
        Map<String, Object> producerProperties = new HashMap<>(getProducerProperties(ClusterUtils.getBroker()));
        producerProperties.putAll(getProducerAdditionalProperties());

        produce(numRecords, producerProperties, topic);
    }

    public static List<RecordMetadata> produce(int numMessages, Map<String, Object> props, String topic) {
        List<Future<RecordMetadata>> futureList = new ArrayList<>(numMessages);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            IntStream.range(1, numMessages + 1).forEach(i ->
                    futureList.add(producer.send(new ProducerRecord<>(topic, "key-" + i, "message-value-" + i)))
            );
        }

        return futureList.stream().map(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }).toList();
    }

    public static Map<String, Object> getProducerProperties(String broker) {
        return Map.of(
                BOOTSTRAP_SERVERS_CONFIG, broker,
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public static Map<String, Object> getProducerAdditionalProperties() {
        return Map.of(
                ACKS_CONFIG, "all",
                BUFFER_MEMORY_CONFIG, "12582912",
                CONNECTIONS_MAX_IDLE_MS_CONFIG, "300000");
    }

    public static void main(String[] args) {
        SimpleProducer.produce(100, "inventory");
    }
}
