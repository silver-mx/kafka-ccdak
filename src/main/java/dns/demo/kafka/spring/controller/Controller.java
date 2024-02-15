package dns.demo.kafka.spring.controller;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.Objects;

import static dns.demo.kafka.spring.streams.Processor.COUNTS_KTABLE_STORE;

@RestController
@RequiredArgsConstructor
public class Controller {

    private final StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/count/{word}")
    public Entry<String, Long> getCount(@PathVariable String word) {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        ReadOnlyKeyValueStore<String, Long> counts = Objects.requireNonNull(kafkaStreams)
                .store(StoreQueryParameters.fromNameAndType(COUNTS_KTABLE_STORE, QueryableStoreTypes.keyValueStore()));

        return new AbstractMap.SimpleEntry<>("count", counts.get(word));
    }
}
