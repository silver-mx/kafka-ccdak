package dns.demo.kafka.util;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.TableInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ListTopicsOptions;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static dns.demo.kafka.java.ksql.QueryStream.TABLE_POSTFIX;
import static dns.demo.kafka.util.ClusterUtils.getAdminClient;
import static dns.demo.kafka.util.ClusterUtils.getKSqlDbClientOptions;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

@Slf4j
public class CleanResourcesUtil {

    public static void cleanAllResources() throws ExecutionException, InterruptedException {
        try (Client client = Client.create(getKSqlDbClientOptions())) {
            cleanAllTopics();
            cleanAllTables(client);
            cleanAllStreams(client);
        }

    }

    private static void cleanAllTopics() throws ExecutionException, InterruptedException {
        Set<String> topicsInCluster = getAdminClient().listTopics(new ListTopicsOptions().listInternal(false))
                .names()
                .get()
                .stream()
                .filter(name -> !name.contains("docker-connect")) // Confluent docker seems to create those
                .filter(name -> !name.startsWith("_")) // Confluent docker seems to create those
                .filter(name -> !name.startsWith("default_ksql_processing_log")) // KSQLDB stuff
                .collect(toSet());

        ClusterUtils.deleteTopics(topicsInCluster);
    }

    private static void cleanAllTables(Client client) throws ExecutionException, InterruptedException {
        Set<String> existingTables = client.listTables().get().stream()
                .collect(toMap(TableInfo::getName, Function.identity()))
                .keySet();

        existingTables.forEach(stream -> dropStreamOrTable(client, stream, existingTables));
    }

    private static void cleanAllStreams(Client client) throws ExecutionException, InterruptedException {
        Set<String> existingStreams = client.listStreams().get().stream()
                .collect(toMap(StreamInfo::getName, Function.identity()))
                .keySet();

        existingStreams.forEach(stream -> dropStreamOrTable(client, stream, existingStreams));
    }


    public static void dropStreamOrTable(Client client, String name, Set<String> existingArtifacts) {
        String artifact = name.contains(TABLE_POSTFIX) || name.contains(TABLE_POSTFIX.toUpperCase()) ? "TABLE" : "STREAM";
        if (existingArtifacts.contains(name.toUpperCase())) {
            try {
                ExecuteStatementResult result = client.executeStatement("DROP " + artifact + " IF EXISTS " + name + ";")
                        .get();
                log.info("Result drop {} {} {}", artifact, name, result.queryId());
            } catch (Exception e) {
                client.close();
                log.error("Error dropping {}[{}]", artifact, name, e);
                throw new RuntimeException(e);
            }
        } else {
            log.info("{} {} does not exist yet, it won't be dropped ...", artifact, name);
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        cleanAllResources();
    }
}
