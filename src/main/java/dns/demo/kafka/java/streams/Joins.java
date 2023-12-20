package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.streams.util.StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Consumer;

@Slf4j
public class Joins {

    public static KafkaStreams joinStreams(Properties props, String leftTopic, String rightTopic, String innerJoinTopic,
                                           String leftJoinTopic, String outerJoinTopic) {
        Consumer<StreamsBuilder> consumer = streamsBuilder -> {
            KStream<String, String> left = streamsBuilder.stream(leftTopic);
            KStream<String, String> right = streamsBuilder.stream(rightTopic);

            left.join(right, (lValue, rValue) -> "left=" + lValue + ", right=" + rValue, JoinWindows.of(Duration.ofMinutes(1)))
                    .to(innerJoinTopic);

            left.leftJoin(right, (lValue, rValue) -> {
                        log.info("leftJoin -> left={}, right={}", lValue, rValue);
                        return "left=" + lValue + ", right=" + rValue;
                    }, JoinWindows.of(Duration.ofMinutes(1)))
                    .to(leftJoinTopic);

            left.outerJoin(right, (lValue, rValue) -> {
                        log.info("outerJoin -> left={}, right={}", lValue, rValue);
                        return "left=" + lValue + ", right=" + rValue;
                    }, JoinWindows.of(Duration.ofMinutes(1)))
                    .to(outerJoinTopic);
        };

        return StreamUtils.executeKafkaStreams(props, consumer);
    }
}
