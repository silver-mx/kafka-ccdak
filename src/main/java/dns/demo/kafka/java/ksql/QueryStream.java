package dns.demo.kafka.java.ksql;

import dns.demo.kafka.java.pubsub.SimpleConsumer;
import dns.demo.kafka.util.ClusterUtils;
import io.confluent.ksql.api.client.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static dns.demo.kafka.util.CleanResourcesUtil.dropStreamOrTable;
import static dns.demo.kafka.util.ClusterUtils.getKSqlDbClientOptions;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toMap;

@Slf4j
public class QueryStream {

    public static final String TABLE_POSTFIX = "_table";
    public static final String STREAM_POSTFIX = "_stream";

    public static final String MEMBER_CONTACT_TOPIC = "member_contact";
    public static final String MEMBER_SIGNUPS_TOPIC = "member_signups";

    public static final String MEMBER_SIGNUPS_STREAM = MEMBER_SIGNUPS_TOPIC + STREAM_POSTFIX;
    public static final String MEMBER_SIGNUPS_TABLE = MEMBER_SIGNUPS_TOPIC + TABLE_POSTFIX;
    public static final String MEMBER_SIGNUPS_TABLE_QUERYABLE = "queryable_" + MEMBER_SIGNUPS_TOPIC + TABLE_POSTFIX;
    public static final String MEMBER_SIGNUPS_EMAIL_STREAM_QUERYABLE = "queryable_" + "member_signups_email" + STREAM_POSTFIX;

    public static final String MEMBER_CONTACT_STREAM = MEMBER_CONTACT_TOPIC + STREAM_POSTFIX;
    public static final String MEMBER_EMAIL_LIST_STREAM = "member_email_list" + STREAM_POSTFIX;
    public static final String MEMBER_EMAIL_LIST_TABLE = MEMBER_EMAIL_LIST_STREAM + TABLE_POSTFIX;

    public static final int EXPECTED_RECORDS = 5;


    public static void createStreams(Client client) {

        // NOTE: The use of KEY/ROWKEY is described here: https://docs.ksqldb.io/en/latest/operate-and-deploy/installation/upgrading/#withkey-syntax-removed

        String createStreamSql = """
                CREATE STREAM %s
                    (id STRING KEY, firstname STRING, lastname STRING, email_notifications BOOLEAN)
                WITH (KAFKA_TOPIC='%s', VALUE_FORMAT='DELIMITED');
                """.formatted(MEMBER_SIGNUPS_STREAM, MEMBER_SIGNUPS_TOPIC);
        createStreamOrTable(client, createStreamSql, MEMBER_SIGNUPS_STREAM);

        createStreamSql = String.format("CREATE STREAM %s AS SELECT * FROM %s WHERE email_notifications=true;", MEMBER_SIGNUPS_EMAIL_STREAM_QUERYABLE, MEMBER_SIGNUPS_STREAM);
        createStreamOrTable(client, createStreamSql, MEMBER_SIGNUPS_EMAIL_STREAM_QUERYABLE);

        createStreamSql = """
                CREATE STREAM %s (id STRING KEY, email STRING)
                  WITH(KAFKA_TOPIC = '%s', VALUE_FORMAT = 'DELIMITED');
                """.formatted(MEMBER_CONTACT_STREAM, MEMBER_CONTACT_TOPIC);
        createStreamOrTable(client, createStreamSql, MEMBER_CONTACT_STREAM);

        createStreamSql = """
                CREATE STREAM %s AS
                    SELECT ms.id, ms.firstname, ms.lastname, mc.email
                    FROM %s AS ms
                    INNER JOIN %s AS mc WITHIN 365 DAYS ON ms.id = mc.id;
                """.formatted(MEMBER_EMAIL_LIST_STREAM, MEMBER_SIGNUPS_STREAM, MEMBER_CONTACT_STREAM);
        createStreamOrTable(client, createStreamSql, MEMBER_EMAIL_LIST_STREAM);
    }

    public static void createTables(Client client) {

        // NOTE: The use of KEY/ROWKEY is described here: https://docs.ksqldb.io/en/latest/operate-and-deploy/installation/upgrading/#withkey-syntax-removed

        String createTableSql = """
                CREATE TABLE %s
                    (id STRING PRIMARY KEY, firstname STRING, lastname STRING, email_notifications BOOLEAN)
                WITH (KAFKA_TOPIC='%s', VALUE_FORMAT='DELIMITED');
                """.formatted(MEMBER_SIGNUPS_TABLE, MEMBER_SIGNUPS_TOPIC);
        createStreamOrTable(client, createTableSql, MEMBER_SIGNUPS_TABLE);

        createTableSql = """
                CREATE TABLE %s AS SELECT * FROM %s;
                """.formatted(MEMBER_SIGNUPS_TABLE_QUERYABLE, MEMBER_SIGNUPS_TABLE);
        createStreamOrTable(client, createTableSql, MEMBER_SIGNUPS_TABLE);


        // It seems there is a bug (query hangs) - BUG: https://github.com/confluentinc/ksql/issues/8953
        createTableSql = """
                CREATE TABLE %s AS SELECT ms_id, COUNT(*) FROM %s GROUP BY ms_id;
                """.formatted(MEMBER_EMAIL_LIST_TABLE, MEMBER_EMAIL_LIST_STREAM);
        //createStreamOrTable(client, createTableSql, MEMBER_EMAIL_LIST_TABLE);
    }

    private static void dropStreams(Client client) throws InterruptedException, ExecutionException {
        Set<String> existingStreams = client.listStreams().get().stream()
                .collect(toMap(StreamInfo::getName, Function.identity()))
                .keySet();

        // Drop necessary streams in the correct order
        dropStreamOrTable(client, MEMBER_EMAIL_LIST_STREAM, existingStreams);
        dropStreamOrTable(client, MEMBER_CONTACT_STREAM, existingStreams);
        dropStreamOrTable(client, MEMBER_SIGNUPS_EMAIL_STREAM_QUERYABLE, existingStreams);
        dropStreamOrTable(client, MEMBER_SIGNUPS_STREAM, existingStreams);
    }

    private static void dropTables(Client client) throws InterruptedException, ExecutionException {
        Set<String> existingTables = client.listTables().get().stream()
                .collect(toMap(TableInfo::getName, Function.identity()))
                .keySet();

        // Drop necessary streams in the correct order
        dropStreamOrTable(client, MEMBER_EMAIL_LIST_TABLE, existingTables);
        dropStreamOrTable(client, MEMBER_SIGNUPS_TABLE_QUERYABLE, existingTables);
        dropStreamOrTable(client, MEMBER_SIGNUPS_TABLE, existingTables);
    }

    private static void createStreamOrTable(Client client, String sql, String name) {
        String artifact = name.contains(TABLE_POSTFIX) ? "TABLE" : "STREAM";
        Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");
        try {
            ExecuteStatementResult result = client.executeStatement(sql, properties).get();
            log.info("Result create {} {} {}", artifact, name, result.queryId());
        } catch (Exception e) {
            client.close();
            log.error("Error creating {}[{}]", artifact, name, e);
            throw new RuntimeException(e);
        }
    }

    public static void executeQuerySync(Client client, String sql) throws ExecutionException, InterruptedException {
        Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");

        log.info("Executing SQL \"{}\"", sql);

        // Synchronous execution (there is an asynchronous version as well)
        StreamedQueryResult streamedQueryResult = client.streamQuery(sql, properties).get();

        Row row;
        do {
            // Block until a new row is available
            row = streamedQueryResult.poll();
            if (nonNull(row)) {
                log.info("Received a row {}", row.values());
            } else {
                log.info("Query has ended.");
            }
        } while (nonNull(row));

    }

    public static void executeQueryAsync(Client client, String sql) {
        Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");

        log.info("Executing SQL \"{}\"", sql);

        client.streamQuery(sql, properties)
                .thenAccept(streamedQueryResult -> {
                    log.info("Query has started. Query ID: {}", streamedQueryResult.queryID());
                    RowSubscriber subscriber = new RowSubscriber();
                    streamedQueryResult.subscribe(subscriber);
                }).exceptionally(e -> {
                    log.error("Request failed", e);
                    return null;
                });
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        Client client = Client.create(getKSqlDbClientOptions());

        try {
            ClusterUtils.deleteTopics(List.of(MEMBER_EMAIL_LIST_STREAM, MEMBER_SIGNUPS_EMAIL_STREAM_QUERYABLE));
            dropTables(client);
            dropStreams(client);
            createStreams(client);
            createTables(client);

            // Seems to be necessary for the stream to work correctly.
            Thread.sleep(Duration.ofSeconds(1));

            if (args[0].equals("--with-ksql-stream-query")) {
                // Run SimpleProducer.main with argument --for-ksql-demo to generate data for this demo
                executeQuerySync(client, "SELECT * FROM " + MEMBER_SIGNUPS_EMAIL_STREAM_QUERYABLE + ";");
                executeQuerySync(client, "SELECT * FROM " + MEMBER_SIGNUPS_TABLE_QUERYABLE + ";");
                client.close();

                // Test consuming from the topics that back a stream and a table
                log.info("*************************** CONSUMING FROM STREAM ***************************");
                int records1 = SimpleConsumer.consume(MEMBER_SIGNUPS_EMAIL_STREAM_QUERYABLE.toUpperCase());
                Validate.isTrue(records1 == 3, "Expected value 3");
                log.info("*************************** CONSUMING FROM TABLE ***************************");
                // NOTE: Every query multiplies the results (multiples of 5), it does not seem reliable to do this.
                int records2 = SimpleConsumer.consume(MEMBER_SIGNUPS_TABLE_QUERYABLE.toUpperCase());
                Validate.isTrue(records2 % 5 == 0, "Expected value to be a multiple of 5");

            } else if (args[0].equals("--with-ksql-join-stream-query")) {
                // Run SimpleProducer.main with argument --for-ksql-demo to generate data for this demo
                executeQueryAsync(client, "SELECT * FROM " + MEMBER_EMAIL_LIST_STREAM + ";");
                //executeQueryAsync(client, "SELECT * FROM " + MEMBER_EMAIL_LIST_TABLE + ";");
                CompletableFuture.delayedExecutor(2, TimeUnit.SECONDS).execute(client::close);
            }
        } catch (Exception e) {
            client.close();
            throw e;
        }
    }
}
