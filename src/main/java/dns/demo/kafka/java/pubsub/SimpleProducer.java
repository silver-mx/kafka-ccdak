package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.util.ClusterUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class SimpleProducer {

    public static void produce(int numRecords, String topic) {
        produce(numRecords, getProducerProperties(ClusterUtils.getBroker()), topic);
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
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }

    public static void main(String[] args) {
        SimpleProducer.produce(100, "inventory");
    }
}
