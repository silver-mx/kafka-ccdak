package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.streams.util.StreamUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Consumer;

public class Windowing {

    public static KafkaStreams groupByHoppingWindow(Properties props, String inputTopic, String outputTopic) {
        Duration windowSize = Duration.ofSeconds(1);
        Duration advanceSize = Duration.ofMillis(500);
        TimeWindows hoppingWindow = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceSize);

        return groupByWindow(props, inputTopic, outputTopic, hoppingWindow);
    }

    public static KafkaStreams groupByTumblingWindow(Properties props, String inputTopic, String outputTopic) {
        Duration windowSize = Duration.ofSeconds(1);
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(windowSize);

        return groupByWindow(props, inputTopic, outputTopic, tumblingWindow);
    }

    public static KafkaStreams groupByWindow(Properties props, String inputTopic, String outputTopic, TimeWindows window) {
        Consumer<StreamsBuilder> consumer = streamsBuilder -> {

            KStream<String, String> source = streamsBuilder.stream(inputTopic);

            source.groupByKey()
                    .windowedBy(window)
                    .reduce((value1, value2) -> value1 + value2)
                    .toStream()
                    .to(outputTopic, Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class, window.size())));
        };

        return StreamUtils.executeKafkaStreams(props, consumer);
    }
}
