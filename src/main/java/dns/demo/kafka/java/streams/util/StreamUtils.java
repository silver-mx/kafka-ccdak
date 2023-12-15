package dns.demo.kafka.java.streams.util;

import dns.demo.kafka.util.ClusterUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;

@Slf4j
public class StreamUtils {
    public static final String INPUT_TOPIC_STREAM = "topic-stream";
    public static final String OUTPUT_TOPIC_STREAM_1 = "topic-stream-output-1";
    public static final String OUTPUT_TOPIC_STREAM_2 = "topic-stream-output-2";

    public static Properties getStreamProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterUtils.getBroker());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        return props;
    }

    public static KafkaStreams executeKafkaStreams(Properties props, Consumer<StreamsBuilder> streamsBuilderConsumer) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilderConsumer.accept(streamsBuilder);
        Topology topology = streamsBuilder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();

        return kafkaStreams;
    }
}
