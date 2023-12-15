package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.streams.util.StreamUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.function.Consumer;

public class StatelessTransformation {

    public static KafkaStreams splitStreamIntoTwoStreams(Properties props, String inputTopic, String outputTopic1, String outputTopic2) {
        Consumer<StreamsBuilder> consumer = streamsBuilder -> {
            KStream<String, String> source = streamsBuilder.stream(inputTopic);
            source.split()
                    .branch((key, value) -> key.startsWith("key-1"),
                            Branched.withConsumer(ks -> ks.to(outputTopic1)))
                    .branch((key, value) -> !key.startsWith("key-1"),
                            Branched.withConsumer(ks -> ks.to(outputTopic2)));
        };

        return StreamUtils.executeKafkaStreams(props, consumer);
    }

    public static KafkaStreams filterStream(Properties streamProperties, String inputTopic, String outputTopic) {
        Consumer<StreamsBuilder> consumer = streamsBuilder ->
                streamsBuilder.stream(inputTopic).filter((key, value) -> ((String) key).startsWith("key-1")).to(outputTopic);
        return StreamUtils.executeKafkaStreams(streamProperties, consumer);
    }

}