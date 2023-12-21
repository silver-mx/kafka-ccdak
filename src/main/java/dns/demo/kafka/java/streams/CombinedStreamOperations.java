package dns.demo.kafka.java.streams;

import dns.demo.kafka.java.streams.util.StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.function.Consumer;

@Slf4j
public class CombinedStreamOperations {

    public static KafkaStreams countTotalPurchases(Properties props, String inventoryPurchasesTopic,
                                                   String countTotalPurchasesTopic) {

        Consumer<StreamsBuilder> consumer = streamsBuilder -> {
            KStream<String, String> source = streamsBuilder.stream(inventoryPurchasesTopic);
            source
                    .mapValues(value -> {
                        try {
                            return Integer.parseInt(value);
                        } catch (NumberFormatException e) {
                            log.error("An error occurred parsing value={}", value);
                            return 0;
                        }
                    })
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                    .reduce((total, newQuantity) -> total + newQuantity)
                    .toStream()
                    .to(countTotalPurchasesTopic, Produced.with(Serdes.String(), Serdes.Integer()));
        };

        return StreamUtils.executeKafkaStreams(props, consumer);
    }
}
