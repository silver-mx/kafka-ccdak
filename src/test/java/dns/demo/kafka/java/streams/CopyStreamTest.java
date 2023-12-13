package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.pubsub.SimpleConsumer;
import dns.demo.kafka.java.pubsub.SimpleProducer;
import dns.demo.kafka.util.ClusterUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static dns.demo.kafka.java.streams.util.StreamUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CopyStreamTest {
    @BeforeEach
    void setUp() {
        ClusterUtils.getAdminClient().deleteTopics(List.of(INPUT_TOPIC_STREAM, OUTPUT_TOPIC_STREAM_1));
    }

    @Test
    void copyDataFromTopicToTopic() {
        int expectedRecords = 100;
        SimpleProducer.produce(expectedRecords, INPUT_TOPIC_STREAM);
        CopyStream.copyDataFromTopicToTopic(getStreamProperties(), INPUT_TOPIC_STREAM, OUTPUT_TOPIC_STREAM_1);
        int recordCount = SimpleConsumer.consume(OUTPUT_TOPIC_STREAM_1);

        assertEquals(expectedRecords, recordCount);
    }
}