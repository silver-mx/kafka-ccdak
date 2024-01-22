package dns.demo.kafka.java.ksql;

import io.confluent.ksql.api.client.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
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

    public static final String MEMBER_SIGNUPS_EMAIL_DEMO_STREAM = "MEMBER_SIGNUPS_EMAIL_DEMO_3";
    public static final String MEMBER_SIGNUPS_DEMO_STREAM = "MEMBER_SIGNUPS_DEMO_3";
    public static final String MEMBER_SIGNUPS_TOPIC = "member_signups_3";

    public static ClientOptions getClientOptions() {
        return ClientOptions.create()
                .setHost(KSQLDB_SERVER_HOST)
                .setPort(KSQLDB_SERVER_HOST_PORT);
    }

    public static void createStreamsIfMissing(Client client) throws ExecutionException, InterruptedException {
        Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");
        Map<String, List<StreamInfo>> ksqlStreams = client.listStreams()
                .get()
                .stream()
                .collect(groupingBy(StreamInfo::getName));

        if (!ksqlStreams.containsKey(MEMBER_SIGNUPS_DEMO_STREAM)) {
            String createStreamSql = """
                    CREATE STREAM %s
                                (firstname VARCHAR,
                                lastname VARCHAR,
                                email_notifications BOOLEAN)
                              WITH (KAFKA_TOPIC='%s',
                                VALUE_FORMAT='DELIMITED');
                    """.formatted(MEMBER_SIGNUPS_DEMO_STREAM, MEMBER_SIGNUPS_TOPIC);
            ExecuteStatementResult memberSignupsDemoResult = client.executeStatement(createStreamSql, properties).get();

            log.info("Result create stream {} {}", MEMBER_SIGNUPS_EMAIL_DEMO_STREAM, memberSignupsDemoResult.queryId());
        } else {
            log.info("Ksql stream {} already exists", MEMBER_SIGNUPS_DEMO_STREAM);
        }

        if (!ksqlStreams.containsKey(MEMBER_SIGNUPS_EMAIL_DEMO_STREAM)) {
            ExecuteStatementResult memberSignupsEmailDemoResult = client.executeStatement("""
                    CREATE STREAM %s AS
                      SELECT * FROM %s WHERE email_notifications=true;
                    """.formatted(MEMBER_SIGNUPS_EMAIL_DEMO_STREAM, MEMBER_SIGNUPS_DEMO_STREAM), properties).get();

            log.info("Result create stream {} {}", MEMBER_SIGNUPS_EMAIL_DEMO_STREAM, memberSignupsEmailDemoResult.queryId());
        } else {
            log.info("Ksql stream {} already exists", MEMBER_SIGNUPS_EMAIL_DEMO_STREAM);
        }
    }

    public static void executeQuery() throws ExecutionException, InterruptedException {
        Client client = Client.create(getClientOptions());
        Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");

        createStreamsIfMissing(client);

        log.info("Executing \"SELECT * FROM {};\"", MEMBER_SIGNUPS_EMAIL_DEMO_STREAM);

        // Synchronous execution (there is an asynchronous version as well)
        StreamedQueryResult streamedQueryResult = client.streamQuery("SELECT * FROM " + MEMBER_SIGNUPS_EMAIL_DEMO_STREAM + ";",
                properties).get();

        /* Run SimpleProducer.main with argument --for-ksql-demo to generate data for this demo, OR do:
         *
         *  kafka-console-producer.sh --broker-list localhost:9092 --topic member_signups_3 --property "parse.key=true" --property "key.separator=:"
         *   >1:last1,name1,true
         *   >2:last2,name2,false
         *   >3:last3,name2,true
         *   >4:last4,name4,true
         *
         * */
        Row row;
        do {
            // Block until a new row is available
            row = streamedQueryResult.poll(Duration.ofSeconds(3));
            if (nonNull(row)) {
                log.info("Received a row {}", row.values());
            } else {
                log.info("Query has ended.");
            }
        } while (nonNull(row));

        // Terminate any open connections and close the client
        client.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        executeQuery();
    }
}
