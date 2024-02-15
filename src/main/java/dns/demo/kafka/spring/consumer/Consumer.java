package dns.demo.kafka.spring.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static dns.demo.kafka.spring.config.KafkaConfig.SPRING_KAFKA_TOPIC_PLAIN;
import static dns.demo.kafka.spring.config.KafkaConfig.SPRING_KAFKA_TOPIC_WORD_COUNT_OUTPUT;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

@Slf4j
@Component
public class Consumer {

    @KafkaListener(topics = {SPRING_KAFKA_TOPIC_PLAIN}, groupId = "spring-boot-consumer-grp")
    public void consumeSimpleProducerStringRecord(
            @Header(name = KafkaHeaders.RECEIVED_KEY) String key,
            @Payload String value,
            @Header(name = KafkaHeaders.RECEIVED_PARTITION) String partition,
            @Header(name = KafkaHeaders.OFFSET) String offset,
            Acknowledgment ack) {
        log.info("Consuming producer record partition={}, offset={}, key={}, value={}", partition, offset, key, value);
        ack.acknowledge(); // manual commit
    }

    @KafkaListener(topics = {SPRING_KAFKA_TOPIC_WORD_COUNT_OUTPUT}, groupId = "spring-boot-consumer-grp",
            properties = {
                    VALUE_DESERIALIZER_CLASS_CONFIG + "=org.apache.kafka.common.serialization.LongDeserializer"
            })
    public void consumeStreamStringRecord(ConsumerRecord<String, Long> record,
                                          Acknowledgment ack) {
        log.info("Consuming stream-output record partition={}, offset={}, key={}, value={}", record.partition(), record.offset(), record.key(), record.value());
        ack.acknowledge(); // manual commit
    }

}
