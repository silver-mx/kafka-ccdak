package dns.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.IntStream;

public class SimpleProducerExample {

    public static void produce(int numMessages) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "dell:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            IntStream.range(1, numMessages + 1).forEach(i ->
                    producer.send(new ProducerRecord<>("count-topic", "count", Integer.toString(i)))
            );
        }
    }

    public static void main(String[] args) {
        SimpleProducerExample.produce(100);
    }
}
