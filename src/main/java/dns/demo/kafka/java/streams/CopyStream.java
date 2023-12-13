package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.pubsub.SimpleConsumer;
import dns.demo.kafka.java.pubsub.SimpleProducer;
import dns.demo.kafka.java.streams.util.StreamUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

import static dns.demo.kafka.java.streams.util.StreamUtils.*;

@Slf4j
public class CopyStream {

    public static void copyDataFromTopicToTopic(Properties props, String inputTopic, String outputTopic) {
        StreamUtils.kafkaStreamsExecute(props, streamsBuilder -> streamsBuilder.stream(inputTopic).to(outputTopic));
    }

    public static void main(String[] args) {
        SimpleProducer.produce(100, INPUT_TOPIC_STREAM);
        CopyStream.copyDataFromTopicToTopic(getStreamProperties(), INPUT_TOPIC_STREAM, OUTPUT_TOPIC_STREAM_1);
        SimpleConsumer.consume(OUTPUT_TOPIC_STREAM_1);
    }
}
