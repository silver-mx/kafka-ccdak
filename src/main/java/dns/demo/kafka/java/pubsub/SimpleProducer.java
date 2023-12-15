package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.util.ClusterUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class SimpleProducer {

    public static void produce(int numRecords, String topic) {
        produce(numRecords, getProducerProperties(ClusterUtils.getBroker()), topic);
    }

    public static void produce(int numMessages, Map<String, Object> props, String topic) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            IntStream.range(1, numMessages + 1).forEach(i ->
                    producer.send(new ProducerRecord<>(topic, "key-" + i, "message-value-" + i))
            );
        }
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
