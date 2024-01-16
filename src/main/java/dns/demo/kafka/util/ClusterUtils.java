package dns.demo.kafka.util;

import org.apache.kafka.clients.admin.AdminClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class ClusterUtils {

    public static final String KAFKA_HOST_DEFAULT = "localhost";
    public static final int KAFKA_PORT_DEFAULT = 9092;
    public static final int KAFKA_TLS_PORT_DEFAULT = 29094;
    public static final int KAFKA_SCHEMA_REGISTRY_PORT_DEFAULT = 8081;
    public static final String KAFKA_HOST_PORT_DEFAULT = String.format("%s:%d", KAFKA_HOST_DEFAULT, KAFKA_PORT_DEFAULT);
    public static final String KAFKA_TLS_HOST_PORT_DEFAULT = String.format("%s:%d", KAFKA_HOST_DEFAULT, KAFKA_TLS_PORT_DEFAULT);
    public static final String KAFKA_SCHEMA_REGISTRY_URL_DEFAULT = String.format("http://%s:%d", KAFKA_HOST_DEFAULT, KAFKA_SCHEMA_REGISTRY_PORT_DEFAULT);


    public static String getClientTruststorePath() {
        File truststore = new File(requireNonNull(ClusterUtils.class.getClassLoader()
                .getResource("tls-certs/client/truststore-client.pkcs12"))
                .getFile());
        return truststore.getAbsolutePath();
    }

    public static String getClientTruststoreCredentials() throws IOException {
        File truststorePass = new File(requireNonNull(ClusterUtils.class.getClassLoader()
                .getResource("tls-certs/client/truststore-creds-client"))
                .getFile());
        return Files.readString(truststorePass.toPath());
    }

    public static String getBroker() {
        return Optional.ofNullable(System.getenv("KAFKA_HOST_PORT")).orElse(KAFKA_HOST_PORT_DEFAULT);
    }

    public static String getBrokerTls() {
        return Optional.ofNullable(System.getenv("KAFKA_TLS_HOST_PORT")).orElse(KAFKA_TLS_HOST_PORT_DEFAULT);
    }

    public static String getSchemaRegistryUrl() {
        return Optional.ofNullable(System.getenv("KAFKA_SCHEMA_REGISTRY_URL")).orElse(KAFKA_SCHEMA_REGISTRY_URL_DEFAULT);
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
