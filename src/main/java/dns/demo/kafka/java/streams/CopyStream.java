package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.streams.util.StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Properties;

@Slf4j
public class CopyStream {

    public static KafkaStreams copyDataFromTopicToTopic(Properties props, String inputTopic, String outputTopic) {
        return StreamUtils.executeKafkaStreams(props, streamsBuilder -> streamsBuilder.stream(inputTopic).to(outputTopic));
    }

}
