package dns.demo.kafka.util;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Optional;
import java.util.Properties;

public class ClusterUtils {

    public static String getBroker() {
        return Optional.ofNullable(System.getenv("KAFKA_HOST_PORT")).orElse("dell:9092");
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
