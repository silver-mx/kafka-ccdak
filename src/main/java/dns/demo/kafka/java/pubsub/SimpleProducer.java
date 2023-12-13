package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.util.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

public class SimpleProducer {

    public static void produce(int numMessages, String topic) {
        produce(numMessages, getProducerProperties(), topic);
    }

    public static void produce(int numMessages, Properties props, String topic) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            IntStream.range(1, numMessages + 1).forEach(i ->
                    producer.send(new ProducerRecord<>(topic, "key-" + i, "message-value-" + i))
            );
        }
    }

    public static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Utils.getClusterHostPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }

    public static void main(String[] args) {
        SimpleProducer.produce(100, getProducerProperties(), "inventory");
    }
}
