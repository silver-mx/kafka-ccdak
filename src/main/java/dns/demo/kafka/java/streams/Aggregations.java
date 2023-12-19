package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.streams.util.StreamUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;
import java.util.function.Consumer;

public class Aggregations {

    public static KafkaStreams groupByKey(Properties props, String inputTopic, String outputTopicCountChars,
                                          String outputTopicCountRecords, String outputTopicReducedString) {
        Consumer<StreamsBuilder> consumer = streamsBuilder -> {
            KStream<String, String> source = streamsBuilder.stream(inputTopic);
            // Count the number of characters for each value with the same key
            source.groupByKey()
                    .aggregate(() -> 0, (key, value, aggregate) -> aggregate + value.length(),
                            /* The materialized.with is required to tell Kafka to use serdes for key=String value=Integer
                            instead of the defaults for the grouped stream which are key=String, value=String.*/
                            Materialized.with(Serdes.String(), Serdes.Integer()))
                    .toStream()
                    .to(outputTopicCountChars);

            // Count the number of records
            source.groupByKey()
                    .count(Materialized.with(Serdes.String(), Serdes.Long()))
                    .toStream()
                    .to(outputTopicCountRecords);

            // Reduce a string
            source.groupByKey()
                    .reduce((value1, value2) -> value1 + "-" + value2)
                    .toStream()
                    .to(outputTopicReducedString);
        };

        return StreamUtils.executeKafkaStreams(props, consumer);
    }
}
