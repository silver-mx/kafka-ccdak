package dns.demo.kafka.java.simple;

import dns.demo.kafka.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class SimpleConsumerExample {

    public static int consume(Properties props, String topic) {
        int recordCount = 0;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long startTime = System.currentTimeMillis();
            while (Duration.ofMillis(System.currentTimeMillis() - startTime).getSeconds() < 30) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record ->
                        log.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value())
                );
                recordCount += records.count();

                if (records.count() > 0) {
                    log.info("Found new {} records", records.count());
                }
            }
            log.info("Found total {} records", recordCount);
        }

        return recordCount;
    }

    private static Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Utils.getClusterHostPort());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "simple-java-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //From the beginning
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return props;
    }

    public static void main(String[] args) {
        int recordCount = SimpleConsumerExample.consume(getConsumerProperties(), "inventory");
        log.info("recordCount=" + recordCount);
    }
}
