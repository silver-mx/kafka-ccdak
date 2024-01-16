package dns.demo.kafka.util;

import dns.demo.kafka.java.streams.util.StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.streams.KafkaStreams;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static dns.demo.kafka.util.ClusterUtils.*;

@Slf4j
public class MiscUtils {

    /**
     * Fragment taken from https://github.com/linuxacademy/content-ccdak-kafka-streams/blob/master/src/main/java/com/linuxacademy/ccdak/streams/StatelessTransformationsMain.java
     *
     * @param supplier
     */
    public static void runUntilCancelled(Supplier<StreamUtils.StreamPair> supplier) {

        StreamUtils.StreamPair streamPair = supplier.get();
        try (KafkaStreams streams = streamPair.kafkaStreams()) {
            CountDownLatch latch = new CountDownLatch(1);

            // Attach a shutdown handler to catch control-c and terminate the application gracefully.
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    latch.countDown();
                }
            });

            try {
                streams.start();
                latch.await();
            } catch (final Throwable e) {
                System.out.println(e.getMessage());
                System.exit(1);
            }

            log.info("The run has been cancelled ....");
            System.exit(0);
        }
    }

    public static Map<String, Object> addTlsConfigurationProperties(Map<String, Object> props) throws IOException {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());

        // For 1-way TLS communication
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, getClientTruststorePath());
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, getClientTruststoreCredentials());
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");

        // For 2-way TLS (mTLS) communication
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, getClientKeystorePath());
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, getClientKeystoreCredentials());
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, getClientSslKeyCredentials());
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");

        return Collections.unmodifiableMap(props);
    }

}
