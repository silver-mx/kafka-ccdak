package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.util.ClusterUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import static java.util.Objects.isNull;

@Slf4j
public class SimpleConsumer {

    public static int consume(String topic) {
        return consume(getConsumerProperties(ClusterUtils.getBroker()), topic);
    }

    public static int consume(Map<String, Object> props, String topic) {
        return consume(props, topic, null);
    }

    public static int consume(Map<String, Object> props, String topic, Integer expectedNumRecords) {
        int recordCount = 0;
        long startTime = System.currentTimeMillis();
        Predicate<Integer> consumeUntilPredicate = count -> {
            boolean shouldWaitLonger = Duration.ofMillis(System.currentTimeMillis() - startTime).compareTo(Duration.ofSeconds(10)) < 0;
            return isNull(expectedNumRecords) ? shouldWaitLonger : count < expectedNumRecords && shouldWaitLonger;
        };

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));

            while (consumeUntilPredicate.test(recordCount)) {
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

    public static Map<String, Object> getConsumerProperties(String broker) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "simple-java-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //From the beginning
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return props;
    }

    public static void main(String[] args) {
        int recordCount = SimpleConsumer.consume(getConsumerProperties(ClusterUtils.getBroker()), "inventory");
        log.info("recordCount=" + recordCount);
    }
}
