package dns.demo.kafka.util;

import org.apache.kafka.clients.admin.AdminClient;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
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
        return getAbsolutePath("tls-certs/client-1/truststore-client-1.pkcs12");
    }

    public static String getClientKeystorePath() {
        return getAbsolutePath("tls-certs/client-1/keystore-client-1.pkcs12");
    }

    private static String getAbsolutePath(String path) {
        URL resource = ClusterUtils.class.getClassLoader().getResource(path);
        File truststore = new File(requireNonNull(resource, "Path[" + path + "] is missing").getFile());
        return truststore.getAbsolutePath();
    }

    public static String getClientTruststoreCredentials() throws IOException {
        return getCredentials("tls-certs/client-1/truststore-creds-client-1");
    }

    public static String getClientKeystoreCredentials() throws IOException {
        return getCredentials("tls-certs/client-1/keystore-creds-client-1");
    }

    public static String getClientSslKeyCredentials() throws IOException {
        return getCredentials("tls-certs/client-1/sslkey-creds-client-1");
    }

    private static String getCredentials(String path) throws IOException {
        return Files.readString(Path.of(getAbsolutePath(path)));
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
