package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.streams.util.StreamUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.util.List;
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

    public static KafkaStreams mergeStreams(Properties props, String inputTopic1, String inputTopic2, String outputTopic) {
        Consumer<StreamsBuilder> consumer = streamsBuilder -> {
            KStream<String, String> source1 = streamsBuilder.stream(inputTopic1);
            KStream<String, String> source2 = streamsBuilder.stream(inputTopic2);
            source1.merge(source2).to(outputTopic);
        };

        return StreamUtils.executeKafkaStreams(props, consumer);
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaStreams kafkaStreams = StatelessTransformation.mergeStreams(StreamUtils.getStreamProperties(), "topic-stream-output-1", "topic-stream-output-2", "merged-topic");
        Thread.sleep(Duration.ofSeconds(30));
        kafkaStreams.close();
    }

    public static KafkaStreams filterStream(Properties streamProperties, String inputTopic, String outputTopic) {
        Consumer<StreamsBuilder> consumer = streamsBuilder ->
                streamsBuilder.stream(inputTopic).filter((key, value) -> ((String) key).startsWith("key-1")).to(outputTopic);
        return StreamUtils.executeKafkaStreams(streamProperties, consumer);
    }

    /**
     * Converts/maps each record into a list of records.
     */
    public static KafkaStreams flatMapStream(Properties streamProperties, String inputTopic, String outputTopic) {
        Consumer<StreamsBuilder> consumer = streamsBuilder ->
                streamsBuilder.stream(inputTopic)
                        .flatMap((key, value) -> List.of(KeyValue.pair(key, value.toString().toLowerCase()),
                                KeyValue.pair(key, value.toString().toUpperCase())))
                        .to(outputTopic);
        return StreamUtils.executeKafkaStreams(streamProperties, consumer);
    }

    /**
     * Converts/maps each record into another single record.
     */
    public static KafkaStreams mapStream(Properties streamProperties, String inputTopic, String outputTopic) {
        Consumer<StreamsBuilder> consumer = streamsBuilder ->
                streamsBuilder.stream(inputTopic)
                        .map((key, value) -> KeyValue.pair(key, value.toString().toUpperCase()))
                        .to(outputTopic);
        return StreamUtils.executeKafkaStreams(streamProperties, consumer);
    }
}
