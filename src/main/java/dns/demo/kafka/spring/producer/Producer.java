package dns.demo.kafka.spring.producer;

import com.github.javafaker.Faker;
import dns.demo.kafka.domain.Hobbit;
import dns.demo.kafka.spring.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
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

    private final KafkaTemplate<String, Hobbit> hobbitKafkaTemplate;
    private final KafkaTemplate<String, String> stringKafkaTemplate;
    private Faker faker;
    private final int numRecords;

    public Producer(final KafkaTemplate<String, Hobbit> hobbitKafkaTemplate,
                    final KafkaTemplate<String, String> stringKafkaTemplate,
                    @Value("${application.producer.num-records:1000}") int numRecords) {
        this.hobbitKafkaTemplate = hobbitKafkaTemplate;
        this.stringKafkaTemplate = stringKafkaTemplate;
        this.numRecords = numRecords;
    }

    @EventListener(ApplicationStartedEvent.class)
    public void generatePlainRecords() {
        faker = Faker.instance();
        Flux<Long> interval = Flux.interval(Duration.ofMillis(1_000));
        Flux<String> quotes = fromStream(Stream.generate(() -> faker.hobbit().quote()));

        Flux.zip(interval, quotes)
                .take(numRecords)
                .map(tuple -> {
                    ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConfig.SPRING_KAFKA_TOPIC_PLAIN, String.valueOf(faker.random().nextInt(42)),
                            tuple.getT2());
                    log.info("Sending plain record {}", record);
                    return stringKafkaTemplate.send(record);
                })
                .blockLast();
    }

    @EventListener(ApplicationStartedEvent.class)
    public void generateAvroRecords() {
        faker = Faker.instance();
        Flux<Long> interval = Flux.interval(Duration.ofMillis(1_000));
        Flux<String> quotes = fromStream(Stream.generate(() -> faker.hobbit().quote()));

        Flux.zip(interval, quotes)
                .take(numRecords)
                .map(tuple -> {
                    ProducerRecord<String, Hobbit> record = new ProducerRecord<>(KafkaConfig.SPRING_KAFKA_TOPIC_AVRO, String.valueOf(faker.random().nextInt(42)),
                            new Hobbit(tuple.getT2()));
                    log.info("Sending avro record {}", record);
                    return hobbitKafkaTemplate.send(record);
                })
                .blockLast();
    }
}
