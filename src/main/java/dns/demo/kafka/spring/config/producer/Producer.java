package dns.demo.kafka.spring.config.producer;

import com.github.javafaker.Faker;
import dns.demo.kafka.domain.Purchase;
import dns.demo.kafka.spring.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.stream.Stream;

import static reactor.core.publisher.Flux.fromStream;

@Slf4j
@Component
public class Producer {

    private final KafkaTemplate<String, Purchase> purchaseKafkaTemplate;
    private final KafkaTemplate<String, String> stringKafkaTemplate;
    private Faker faker;

    public Producer(final KafkaTemplate<String, Purchase> purchaseKafkaTemplate,
                    final KafkaTemplate<String, String> stringKafkaTemplate) {
        this.purchaseKafkaTemplate = purchaseKafkaTemplate;
        this.stringKafkaTemplate = stringKafkaTemplate;
    }

    @EventListener(ApplicationStartedEvent.class)
    public void generate() {
        faker = Faker.instance();
        Flux<Long> interval = Flux.interval(Duration.ofMillis(1_000));
        Flux<String> quotes = fromStream(Stream.generate(() -> faker.hobbit().quote()));

        Flux.zip(interval, quotes)
                .map(tuple -> {
                    ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConfig.SPRING_KAFKA_TOPIC_PLAIN, String.valueOf(faker.random().nextInt(42)),
                            tuple.getT2());
                    log.info("Sending plain record {}", record);
                    return stringKafkaTemplate.send(record);
                })
                .blockLast();
    }
}
