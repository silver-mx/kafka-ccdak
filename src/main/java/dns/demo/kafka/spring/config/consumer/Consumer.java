package dns.demo.kafka.spring.config.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static dns.demo.kafka.spring.config.KafkaConfig.SPRING_KAFKA_TOPIC_PLAIN;

@Slf4j
@Component
public class Consumer {

    @KafkaListener(topics = {SPRING_KAFKA_TOPIC_PLAIN}, groupId = "spring-boot-kafka-string-consumer-grp")
    public void consumerStringRecord(
            @Header(name = KafkaHeaders.RECEIVED_KEY) String key,
            @Payload String value,
            @Header(name = KafkaHeaders.RECEIVED_PARTITION) String partition,
            @Header(name = KafkaHeaders.OFFSET) String offset, Acknowledgment ack) {
        log.info("Consuming record partition={}, offset={}, key={}, value={}", partition, offset, key, value);
        ack.acknowledge(); // manual commit
    }

}
