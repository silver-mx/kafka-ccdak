package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.pubsub.SimpleConsumer;
import dns.demo.kafka.java.pubsub.SimpleProducer;
import dns.demo.kafka.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class CopyStream {

    public static void copyDataFromTopicToTopic(Properties props, String inputTopic, String outputTopic) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream(inputTopic).to(outputTopic);

        Topology topology = streamsBuilder.build();
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, props)) {
            log.info("Topology description: {}", topology.describe());

            /* Attach shutdown handler to catch control-c and terminate the application gracefully.
             * The countdown latch will keep the application running.
             * */
            CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    kafkaStreams.close();
                    latch.countDown();
                }
            });

            kafkaStreams.start();

            try {
                latch.await();
            } catch (InterruptedException e) {
                log.error("Error waiting for the latch", e);
                System.exit(1);
            }

            System.exit(0);
        }
    }

    private static Properties getStreamProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Utils.getClusterHostPort());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        return props;
    }

    public static void main(String[] args) {
        String inputTopic = "topic-a";
        String outputTopic = "topic-a-copy";

        SimpleProducer.produce(100, inputTopic);
        CopyStream.copyDataFromTopicToTopic(getStreamProperties(), inputTopic, outputTopic);
        int recordCount = SimpleConsumer.consume(outputTopic);

        assert recordCount == 100;
    }
}
