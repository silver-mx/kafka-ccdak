package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.pubsub.SimpleConsumer;
import dns.demo.kafka.java.pubsub.SimpleProducer;
import dns.demo.kafka.util.ClusterUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static dns.demo.kafka.java.streams.util.StreamUtils.*;
import static dns.demo.kafka.java.streams.util.StreamUtils.OUTPUT_TOPIC_STREAM_2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StatelessTransformationTest {

    @BeforeEach
    void setUp() {
        ClusterUtils.getAdminClient().deleteTopics(List.of(INPUT_TOPIC_STREAM, OUTPUT_TOPIC_STREAM_1, OUTPUT_TOPIC_STREAM_2));
    }

    @Test
    void splitStreamIntoTwoStreams() {
        int expectedRecords = 100;
        SimpleProducer.produce(expectedRecords, INPUT_TOPIC_STREAM);
        StatelessTransformation.splitStreamIntoTwoStreams(getStreamProperties(), INPUT_TOPIC_STREAM, OUTPUT_TOPIC_STREAM_1, OUTPUT_TOPIC_STREAM_2);
        int recordCount1 = SimpleConsumer.consume(OUTPUT_TOPIC_STREAM_1);
        int recordCount2 = SimpleConsumer.consume(OUTPUT_TOPIC_STREAM_2);

        assertThat(recordCount1).isEqualTo(12);
        assertThat(recordCount2).isEqualTo(88);
    }
}