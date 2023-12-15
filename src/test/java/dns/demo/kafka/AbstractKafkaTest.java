package dns.demo.kafka;

import dns.demo.kafka.java.pubsub.SimpleConsumer;
import dns.demo.kafka.java.pubsub.SimpleProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.Properties;

public class AbstractKafkaTest {

    public void produceRecords(int numRecords, String topic, EmbeddedKafkaBroker broker) {
        Properties props = SimpleProducer.getProducerProperties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        SimpleProducer.produce(numRecords, props, topic);
    }

    public int consumeRecords(String topic, EmbeddedKafkaBroker broker) {
        Properties props = SimpleConsumer.getConsumerProperties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        return SimpleConsumer.consume(props, topic);
    }
}
