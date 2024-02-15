package dns.demo.kafka.spring.config;

import dns.demo.kafka.domain.Purchase;
import dns.demo.kafka.java.pubsub.SimpleConsumer;
import dns.demo.kafka.java.pubsub.SimpleProducer;
import dns.demo.kafka.util.ClusterUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

@EnableKafka
@EnableKafkaStreams
@Configuration
public class KafkaConfig {

    public static final String SPRING_KAFKA_TOPIC_PLAIN = "spring-kafka-topic-plain";
    public static final String SPRING_KAFKA_TOPIC_WORD_COUNT_OUTPUT = "streams-wordcount-output";
    public static final String SPRING_KAFKA_TOPIC_AVRO = "spring-kafka-topic-avro";

    private final String bootstrapServers;
    private final String securityProtocol;

    public KafkaConfig(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                       @Value("${spring.kafka.security.protocol}") String securityProtocol) {
        this.bootstrapServers = bootstrapServers;
        this.securityProtocol = securityProtocol;
    }

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
                        .build(),
                TopicBuilder.name(SPRING_KAFKA_TOPIC_WORD_COUNT_OUTPUT)
                        .replicas(1)
                        .partitions(3)
                        .build()
        );
    }

    @Bean
    @Qualifier("avro-producer-config")
    public Map<String, Object> avroProducerConfig() {
        try {
            Map<String, Object> config = new HashMap<>(SimpleProducer.getProducerPropertiesWithTlsAndAvroSerializer(bootstrapServers, ClusterUtils.getSchemaRegistryUrl()));
            config.put(SECURITY_PROTOCOL_CONFIG, securityProtocol);
            return config;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Bean
    @Qualifier("plain-producer-config")
    public Map<String, Object> plainProducerConfig() {
        try {
            Map<String, Object> config = new HashMap<>(SimpleProducer.getProducerPropertiesWithTls(bootstrapServers));
            config.put(SECURITY_PROTOCOL_CONFIG, securityProtocol);
            return config;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Bean
    @Qualifier("plain-consumer-config")
    public Map<String, Object> plainConsumerConfig() {
        try {
            Map<String, Object> config = new HashMap<>(SimpleConsumer.getConsumerPropertiesWithTls(bootstrapServers));
            config.put(SECURITY_PROTOCOL_CONFIG, securityProtocol);
            config.put(ENABLE_AUTO_COMMIT_CONFIG, false);
            return config;
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

    /*@Bean(name = DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>(StreamUtils.getStreamPropertiesMap());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "spring-boot-streams");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.remove(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG);
        props.remove(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG);

        return new KafkaStreamsConfiguration(props);
    }*/

    /* NOTE: The default stream builder provided by spring starts the streams automatically */
    /*@Bean
    public FactoryBean<StreamsBuilder> kStreamBuilder(@Qualifier(DEFAULT_STREAMS_CONFIG_BEAN_NAME) KafkaStreamsConfiguration streamsConfig) {
        return new StreamsBuilderFactoryBean(streamsConfig);
    }*/
}
