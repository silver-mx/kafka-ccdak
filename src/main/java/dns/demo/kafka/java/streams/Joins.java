package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.pubsub.SimpleProducer;
import dns.demo.kafka.java.streams.util.StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Consumer;

@Slf4j
public class Joins {

    public static StreamUtils.StreamPair joinStreams(Properties props, String leftTopic, String rightTopic, String innerJoinTopic,
                                                     String leftJoinTopic, String outerJoinTopic) {
        Consumer<StreamsBuilder> consumer = streamsBuilder -> {
            KStream<String, String> left = streamsBuilder.stream(leftTopic);
            KStream<String, String> right = streamsBuilder.stream(rightTopic);

            left.join(right, (lValue, rValue) -> "left=" + lValue + ", right=" + rValue, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)))
                    .to(innerJoinTopic);

            left.leftJoin(right, (lValue, rValue) -> {
                        log.info("leftJoin -> left={}, right={}", lValue, rValue);
                        return "left=" + lValue + ", right=" + rValue;
                    }, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(100)))
                    .to(leftJoinTopic);

            left.outerJoin(right, (lValue, rValue) -> {
                        log.info("outerJoin -> left={}, right={}", lValue, rValue);
                        return "left=" + lValue + ", right=" + rValue;
                    }, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(100)))
                    .to(outerJoinTopic);
        };

        return StreamUtils.executeKafkaStreamsAndGetTopology(props, consumer);
    }


    public static void main(String[] args) {
        String leftTopic = "left-topic";
        String rightTopic = "right-topic";
        String innerJoinTopic = "inner-join-topic";
        String leftJoinTopic = "left-join-topic";
        String outerJoinTopic = "outer-join-topic";

        SimpleProducer.produce(10, leftTopic);
        SimpleProducer.produce(5, rightTopic);

        StreamUtils.StreamPair pair = Joins.joinStreams(StreamUtils.getStreamProperties(), leftTopic, rightTopic, innerJoinTopic, leftJoinTopic, outerJoinTopic);

        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        pair.kafkaStreams().close();
    }
}
