package dns.demo.kafka.java.ksql;

import dns.demo.kafka.util.ClusterUtils;
import io.confluent.ksql.api.client.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static dns.demo.kafka.util.ClusterUtils.KSQLDB_SERVER_HOST;
import static dns.demo.kafka.util.ClusterUtils.KSQLDB_SERVER_HOST_PORT;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.groupingBy;

@Slf4j
public class QueryStream {

    public static final String MEMBER_CONTACT_TOPIC = "member_contact";
    public static final String MEMBER_SIGNUPS_TOPIC = "member_signups";

    public static final String MEMBER_SIGNUPS_STREAM = MEMBER_SIGNUPS_TOPIC;
    public static final String MEMBER_SIGNUPS_EMAIL_STREAM = "member_signups_email";

    public static final String MEMBER_CONTACT_STREAM = MEMBER_CONTACT_TOPIC;
    public static final String MEMBER_EMAIL_LIST_STREAM = "member_email_list";
    public static final int EXPECTED_RECORDS = 5;


    public static ClientOptions getClientOptions() {
        return ClientOptions.create()
                .setHost(KSQLDB_SERVER_HOST)
                .setPort(KSQLDB_SERVER_HOST_PORT);
    }

    public static void createStreams(Client client) throws ExecutionException, InterruptedException {

        String createStreamSql = """
                CREATE STREAM %s
                    (ROWKEY STRING KEY, firstname VARCHAR, lastname VARCHAR, email_notifications BOOLEAN)
                WITH (KAFKA_TOPIC='%s', VALUE_FORMAT='DELIMITED');
                """.formatted(MEMBER_SIGNUPS_STREAM, MEMBER_SIGNUPS_TOPIC);
        createStream(client, createStreamSql, MEMBER_SIGNUPS_STREAM);

        createStreamSql = String.format("CREATE STREAM %s AS SELECT * FROM %s WHERE email_notifications=true;",
                MEMBER_SIGNUPS_EMAIL_STREAM, MEMBER_SIGNUPS_STREAM);
        createStream(client, createStreamSql, MEMBER_SIGNUPS_EMAIL_STREAM);

        createStreamSql = """
                CREATE STREAM %s (ROWKEY STRING KEY, email VARCHAR)
                  WITH(KAFKA_TOPIC = '%s', VALUE_FORMAT = 'DELIMITED');
                """.formatted(MEMBER_CONTACT_STREAM, MEMBER_CONTACT_TOPIC);
        createStream(client, createStreamSql, MEMBER_CONTACT_STREAM);

        createStreamSql = """
                CREATE STREAM %s AS
                    SELECT ms.ROWKEY, ms.firstname, ms.lastname, mc.email
                    FROM %s AS ms
                    INNER JOIN %s AS mc WITHIN 365 DAYS ON ms.ROWKEY = mc.ROWKEY;
                """.formatted(MEMBER_EMAIL_LIST_STREAM, MEMBER_SIGNUPS_STREAM, MEMBER_CONTACT_STREAM);
        createStream(client, createStreamSql, MEMBER_EMAIL_LIST_STREAM);
    }

    private static void dropStreams(Client client) throws InterruptedException, ExecutionException {
        Map<String, List<StreamInfo>> ksqlStreams = client.listStreams()
                .get()
                .stream()
                .collect(groupingBy(StreamInfo::getName));

        // Drop necessary streams in the correct order
        dropStream(client, MEMBER_EMAIL_LIST_STREAM, ksqlStreams);
        dropStream(client, MEMBER_CONTACT_STREAM, ksqlStreams);
        dropStream(client, MEMBER_SIGNUPS_EMAIL_STREAM, ksqlStreams);
        dropStream(client, MEMBER_SIGNUPS_STREAM, ksqlStreams);
    }

    private static void createStream(Client client, String sql, String streamName) {
        Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");
        try {
            ExecuteStatementResult result = client.executeStatement(sql, properties).get();
            log.info("Result create stream {} {}", streamName, result.queryId());
        } catch (Exception e) {
            client.close();
            log.error("Error creating stream[{}]", streamName, e);
            throw new RuntimeException(e);
        }
    }

    private static void dropStream(Client client, String streamName, Map<String, List<StreamInfo>> ksqlStreams) {
        if (ksqlStreams.containsKey(streamName.toUpperCase())) {
            try {
                ExecuteStatementResult result = client.executeStatement("DROP STREAM IF EXISTS " + streamName + ";").get();
                log.info("Result drop stream {} {}", streamName, result.queryId());
            } catch (Exception e) {
                client.close();
                log.error("Error dropping stream[{}]", streamName, e);
                throw new RuntimeException(e);
            }
        } else {
            log.info("Stream {} does not exist yet, it won't be dropped ...", streamName);
        }
    }

    public static void executeQuerySync(String sql, int expectedRecords) throws ExecutionException, InterruptedException {
        Client client = Client.create(getClientOptions());
        Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");

        ClusterUtils.deleteTopics(List.of(MEMBER_EMAIL_LIST_STREAM, MEMBER_SIGNUPS_EMAIL_STREAM));
        dropStreams(client);
        createStreams(client);

        log.info("Executing \"{}\"", sql);

        // Synchronous execution (there is an asynchronous version as well)
        StreamedQueryResult streamedQueryResult = client.streamQuery(sql, properties).get();

        int fetchedRecords = expectedRecords;
        Row row;
        do {
            // Block until a new row is available
            row = streamedQueryResult.poll();
            if (nonNull(row)) {
                fetchedRecords--;
                log.info("Received a row {}", row.values());
            } else {
                log.info("Query has ended.");
            }
        } while (fetchedRecords > 0);

        // Terminate any open connections and close the client
        client.close();
    }

    public static void executeQueryAsync(String sql) throws ExecutionException, InterruptedException {
        Client client = Client.create(getClientOptions());
        Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");

        ClusterUtils.deleteTopics(List.of(MEMBER_EMAIL_LIST_STREAM, MEMBER_SIGNUPS_EMAIL_STREAM));
        dropStreams(client);
        createStreams(client);

        log.info("Executing \"{}\"", sql);

        Thread.sleep(1000);

        client.streamQuery(sql, properties).thenAccept(streamedQueryResult -> {
            log.info("Query has started. Query ID: {}", streamedQueryResult.queryID());
            // Terminate the client connection inside RowSubscriber
            RowSubscriber subscriber = new RowSubscriber(client);
            streamedQueryResult.subscribe(subscriber);
        }).exceptionally(e -> {
            log.error("Request failed", e);
            return null;
        });
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        if (args[0].equals("--with-ksql-stream-query")) {
            // Run SimpleProducer.main with argument --for-ksql-demo to generate data for this demo
            String sql = "SELECT * FROM " + MEMBER_SIGNUPS_EMAIL_STREAM + ";";
            executeQuerySync(sql, 3);
        } else if (args[0].equals("--with-ksql-join-stream-query")) {
            // Run SimpleProducer.main with argument --for-ksql-demo to generate data for this demo
            String sql = "SELECT * FROM " + MEMBER_EMAIL_LIST_STREAM + ";";
            executeQueryAsync(sql);
        }
    }
}
