package dns.demo.kafka.java.streams.util;

import dns.demo.kafka.util.ClusterUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.kafka.streams.StreamsConfig.WINDOW_SIZE_MS_CONFIG;

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

    public static Map<String, Object> getStreamPropertiesMap() {
        return getStreamProperties().entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(x -> (String) x.getKey(), Map.Entry::getValue));
    }

    public static KafkaStreams executeKafkaStreams(Properties props, Consumer<StreamsBuilder> streamsBuilderConsumer) {
        return executeKafkaStreamsAndGetTopology(props, streamsBuilderConsumer).kafkaStreams();
    }

    public static StreamPair executeKafkaStreamsAndGetTopology(Properties props, Consumer<StreamsBuilder> streamsBuilderConsumer) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilderConsumer.accept(streamsBuilder);
        Topology topology = streamsBuilder.build();

        return new StreamPair(new KafkaStreams(topology, props), topology);
    }

    public record StreamPair(KafkaStreams kafkaStreams, Topology topology) {
    }

}
