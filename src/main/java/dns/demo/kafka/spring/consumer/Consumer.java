package dns.demo.kafka.spring.consumer;

import dns.demo.kafka.domain.Hobbit;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static dns.demo.kafka.spring.config.KafkaConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

@Slf4j
@Component
public class Consumer {

    @KafkaListener(topics = {SPRING_KAFKA_TOPIC_PLAIN}, groupId = "spring-boot-consumer-plain-grp")
    public void consumeSimpleProducerStringRecord(
            @Header(name = KafkaHeaders.RECEIVED_KEY) String key,
            @Payload String value,
            @Header(name = KafkaHeaders.RECEIVED_PARTITION) String partition,
            @Header(name = KafkaHeaders.OFFSET) String offset,
            Acknowledgment ack) {
        log.info("Consuming producer record partition={}, offset={}, key={}, value={}", partition, offset, key, value);
        ack.acknowledge(); // manual commit
    }

    @KafkaListener(topics = {SPRING_KAFKA_TOPIC_AVRO}, groupId = "spring-boot-consumer-avro-grp",
            properties = {
                    VALUE_DESERIALIZER_CLASS_CONFIG + "=io.confluent.kafka.serializers.KafkaAvroDeserializer",
                    AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS + "=true",
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG + "=${spring.kafka.properties.schema.registry.url}"
            }
    )

    public void consumeSimpleProducerAvroRecord(ConsumerRecord<String, Hobbit> record, Acknowledgment ack) {
        log.info("Consuming avro record partition={}, offset={}, key={}, value={}", record.partition(), record.offset(), record.key(), record.value());
        ack.acknowledge(); // manual commit
    }

    @KafkaListener(topics = {SPRING_KAFKA_TOPIC_WORD_COUNT_OUTPUT}, groupId = "spring-boot-consumer-stream-grp",
            properties = {
                    VALUE_DESERIALIZER_CLASS_CONFIG + "=org.apache.kafka.common.serialization.LongDeserializer"
            }
    )
    public void consumeStreamStringRecord(ConsumerRecord<String, Long> record,
                                          Acknowledgment ack) {
        log.info("Consuming stream-output record partition={}, offset={}, key={}, value={}", record.partition(), record.offset(), record.key(), record.value());
        ack.acknowledge(); // manual commit
    }

}
