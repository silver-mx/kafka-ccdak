package dns.demo.kafka.java.streams.util;

import dns.demo.kafka.util.ClusterUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Slf4j
public class StreamUtils {
    public static final String INPUT_TOPIC_STREAM = "topic-stream";
    public static final String OUTPUT_TOPIC_STREAM_1 = "topic-stream-output-1";
    public static final String OUTPUT_TOPIC_STREAM_2 = "topic-stream-output-2";


    public static Properties getStreamProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterUtils.getClusterHostPort());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        return props;
    }

    public static void kafkaStreamsExecute(Properties props, Consumer<StreamsBuilder> streamsBuilderConsumer) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilderConsumer.accept(streamsBuilder);
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
                    latch.countDown();
                }
            });

            // Wait 30 seconds and terminate the execution
            Executors.newSingleThreadExecutor().submit(() -> {
                try {
                    Thread.sleep(Duration.ofSeconds(30));
                    latch.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            kafkaStreams.start();

            try {
                latch.await();
                log.info("Stream work completed ....");
            } catch (InterruptedException e) {
                log.error("Error waiting for the latch", e);
            }
        }
    }
}
