package dns.demo.kafka.util;

import java.util.Optional;

public class ClusterUtils {

    public static String getClusterHostPort() {
        return Optional.ofNullable(System.getenv("KAFKA_HOST_PORT")).orElse("dell:9092");
    }
}
