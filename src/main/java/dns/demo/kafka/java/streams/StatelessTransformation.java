package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.pubsub.SimpleConsumer;
import dns.demo.kafka.java.pubsub.SimpleProducer;
import dns.demo.kafka.java.streams.util.StreamUtils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.function.Consumer;

import static dns.demo.kafka.java.streams.util.StreamUtils.*;
import static dns.demo.kafka.java.streams.util.StreamUtils.OUTPUT_TOPIC_STREAM_1;

public class StatelessTransformation {

    public static void splitStreamIntoTwoStreams(Properties props, String inputTopic, String outputTopic1, String outputTopic2) {
        Consumer<StreamsBuilder> consumer = streamsBuilder -> {
            KStream<String, String> source = streamsBuilder.stream(inputTopic);
            source.split()
                    .branch((key, value) -> key.startsWith("key-1"),
                            Branched.withConsumer(ks -> ks.to(outputTopic1)))
                    .branch((key, value) -> !key.startsWith("key-1"),
                            Branched.withConsumer(ks -> ks.to(outputTopic2)));
        };

        StreamUtils.kafkaStreamsExecute(props, consumer);
    }

    public static void main(String[] args) {
        SimpleProducer.produce(100, INPUT_TOPIC_STREAM);
        StatelessTransformation.splitStreamIntoTwoStreams(getStreamProperties(), INPUT_TOPIC_STREAM, OUTPUT_TOPIC_STREAM_1, OUTPUT_TOPIC_STREAM_2);
        int recordCount1 = SimpleConsumer.consume(OUTPUT_TOPIC_STREAM_1);
        int recordCount2 = SimpleConsumer.consume(OUTPUT_TOPIC_STREAM_2);
    }
}
