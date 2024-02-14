package dns.demo.kafka.spring.config;

import dns.demo.kafka.domain.Purchase;
import dns.demo.kafka.java.pubsub.SimpleConsumer;
import dns.demo.kafka.java.pubsub.SimpleProducer;
import dns.demo.kafka.util.ClusterUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

@Configuration
public class KafkaConfig {

    public static final String SPRING_KAFKA_TOPIC_PLAIN = "spring-kafka-topic-plain";
    public static final String SPRING_KAFKA_TOPIC_AVRO = "spring-kafka-topic-avro";

    @Bean
    public KafkaAdmin.NewTopics topics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(SPRING_KAFKA_TOPIC_PLAIN)
                        .replicas(1)
                        .partitions(3)
                        .build(),
                TopicBuilder.name(SPRING_KAFKA_TOPIC_AVRO)
                        .replicas(1)
                        .partitions(3)
                        .build());
    }

    @Bean
    @Qualifier("avro-producer-config")
    public Map<String, Object> avroProducerConfig() {
        try {
            return SimpleProducer.getProducerPropertiesWithTlsAndAvroSerializer(ClusterUtils.getBrokerTls(), ClusterUtils.getSchemaRegistryUrl());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Bean
    @Qualifier("plain-producer-config")
    public Map<String, Object> plainProducerConfig() {
        try {
            return SimpleProducer.getProducerPropertiesWithTls(ClusterUtils.getBrokerTls());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Bean
    @Qualifier("plain-consumer-config")
    public Map<String, Object> plainConsumerConfig() {
        try {
            Map<String, Object> props = new HashMap<>(SimpleConsumer.getConsumerPropertiesWithTls(ClusterUtils.getBrokerTls()));
            props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
            return props;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Bean
    public ProducerFactory<String, Purchase> purchaseProducerFactory() {
        return new DefaultKafkaProducerFactory<>(avroProducerConfig());
    }

    @Bean
    public ProducerFactory<String, String> stringProducerFactory() {
        return new DefaultKafkaProducerFactory<>(plainProducerConfig());
    }

    /* NOTE: There is no kafka template for consuming messages, but it is @KafkaListener who manages the consumption. */
    @Bean
    public ConsumerFactory<String, String> stringConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(plainConsumerConfig());
    }

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(stringConsumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL); // Manual commit

        return factory;
    }

    @Bean
    public KafkaTemplate<String, Purchase> purchaseKafkaTemplate(ProducerFactory<String, Purchase> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public KafkaTemplate<String, String> stringKafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
