package dns.demo.kafka.util;

import dns.demo.kafka.java.pubsub.SimpleProducer;
import org.apache.kafka.clients.admin.AdminClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class ClusterUtils {

    public static String getClientTruststorePath() {
        File truststore = new File(requireNonNull(ClusterUtils.class.getClassLoader()
                .getResource("tls-certs/client/kafka.client-truststore.pkcs12"))
                .getFile());
        return truststore.getAbsolutePath();
    }

    public static String getClientTruststorePass() throws IOException {
        File truststorePass = new File(requireNonNull(ClusterUtils.class.getClassLoader()
                .getResource("tls-certs/client/client_truststore_creds"))
                .getFile());
        return Files.readString(truststorePass.toPath());
    }

    public static String getBroker() {
        return Optional.ofNullable(System.getenv("KAFKA_HOST_PORT")).orElse("dell:9092");
    }

    public static String getBrokerTls() {
        return Optional.ofNullable(System.getenv("KAFKA_HOST_PORT_TLS")).orElse("dell:29094");
    }

    public static String getSchemaRegistryUrl() {
        return Optional.ofNullable(System.getenv("CONFLUENT_SCHEMA_REGISTRY_URL")).orElse("http://dell:8081");
    }

    public static Properties getAdminClientProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", getBroker());
        return props;
    }

    public static AdminClient getAdminClient() {
        return AdminClient.create(getAdminClientProperties());
    }
}
