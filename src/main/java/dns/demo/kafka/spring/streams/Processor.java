package dns.demo.kafka.spring.streams;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import java.util.Arrays;

import static dns.demo.kafka.spring.config.KafkaConfig.SPRING_KAFKA_TOPIC_PLAIN;
import static dns.demo.kafka.spring.config.KafkaConfig.SPRING_KAFKA_TOPIC_WORD_COUNT_OUTPUT;

@Slf4j
@Component
public class Processor {

    public static final String COUNTS_KTABLE_STORE = "counts";
    private final StreamsBuilder streamsBuilder;

    public Processor(StreamsBuilder streamsBuilder) {
        this.streamsBuilder = streamsBuilder;
    }

    @PostConstruct
    public void process() {
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> textLines = streamsBuilder.stream(SPRING_KAFKA_TOPIC_PLAIN, Consumed.with(stringSerde, stringSerde));

        KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, value) -> value, Grouped.with(stringSerde, stringSerde))
                .count(Materialized.as(COUNTS_KTABLE_STORE));

        wordCounts.toStream()
                .peek((key, value) -> log.info("Stream wordcount key={}, value={}", key, value))
                .to(SPRING_KAFKA_TOPIC_WORD_COUNT_OUTPUT, Produced.with(stringSerde, longSerde));
    }
}
