package dns.demo.kafka.java.pubsub;

import dns.demo.kafka.util.ClusterUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Objects.isNull;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Slf4j
public class SimpleConsumer {

    public static int consume(String topic) {
        return consume(getConsumerProperties(ClusterUtils.getBroker()), topic);
    }

    public static int consume(Map<String, Object> props, String topic) {
        return consume(props, topic, null);
    }

    public static int consume(Map<String, Object> props, String topic, Integer expectedNumRecords) {
        return consume(props, List.of(topic), expectedNumRecords);
    }

    public static int consume(Map<String, Object> props, List<String> topics, Integer expectedNumRecords) {
        Consumer<ConsumerRecord<String, String>> recordConsumer = record ->
                log.info("partition={}, offset={}, key={}, value={}", record.partition(),
                        record.offset(), record.key(), record.value());
        return consume(props, topics, expectedNumRecords, recordConsumer);
    }

    public static int consume(Map<String, Object> props, List<String> topics, Integer expectedNumRecords,
                              Consumer<ConsumerRecord<String, String>> recordConsumer) {
        int recordCount = 0;
        long startTime = System.currentTimeMillis();
        Predicate<Integer> consumeUntilPredicate = count -> {
            boolean shouldWaitLonger = Duration.ofMillis(System.currentTimeMillis() - startTime).compareTo(Duration.ofSeconds(10)) < 0;
            return isNull(expectedNumRecords) ? shouldWaitLonger : count < expectedNumRecords && shouldWaitLonger;
        };

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(topics);

            while (consumeUntilPredicate.test(recordCount)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(recordConsumer);
                recordCount += records.count();
                boolean isManualCommitRequired = props.get(ENABLE_AUTO_COMMIT_CONFIG).equals(Boolean.FALSE);

                if (records.count() > 0 && isManualCommitRequired) {
                    consumer.commitSync();
                    log.info("Committed synchronously the consumed records ....");
                }

                if (records.count() > 0) {
                    log.info("Found new {} records", records.count());
                }
            }
            log.info("Found total {} records", recordCount);
        }

        return recordCount;
    }

    public static Map<String, Object> getConsumerProperties(String broker) {
        return Map.of(
                BOOTSTRAP_SERVERS_CONFIG, broker,
                GROUP_ID_CONFIG, "simple-java-group-" + UUID.randomUUID(),
                ENABLE_AUTO_COMMIT_CONFIG, true,
                AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000,
                AUTO_OFFSET_RESET_CONFIG, "earliest", //From the beginning
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        );
    }

    public static Map<String, Object> getManualCommitConsumerProperties(String broker) {
        Map<String, Object> props = new HashMap<>(getConsumerProperties(broker));
        props.put(ENABLE_AUTO_COMMIT_CONFIG, false);

        return Collections.unmodifiableMap(props);
    }

    public static void main(String[] args) {
        int recordCount = -1;

        if (args.length == 0) {
            recordCount = SimpleConsumer.consume("inventory");
        } else if (args[0].equals("--manual-commit")) {
            recordCount = SimpleConsumer.consume(getManualCommitConsumerProperties(ClusterUtils.getBroker()), "inventory");
        }

        log.info("recordCount=" + recordCount);
    }
}
