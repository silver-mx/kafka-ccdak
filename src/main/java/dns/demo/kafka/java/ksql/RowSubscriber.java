package dns.demo.kafka.java.ksql;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.Row;
import io.confluent.shaded.org.reactivestreams.Subscriber;
import io.confluent.shaded.org.reactivestreams.Subscription;
import lombok.extern.slf4j.Slf4j;


/**
 * To consume records asynchronously, create a Reactive Streams subscriber to receive query result rows.
 */
@Slf4j
public class RowSubscriber implements Subscriber<Row> {

    private Subscription subscription;
    private final Client client;

    public RowSubscriber(Client client) {
        this.client = client;
    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
        log.info("Subscriber is subscribed.");
        this.subscription = subscription;

        // Request the first row
        subscription.request(1);
    }

    @Override
    public synchronized void onNext(Row row) {
        log.info("Received a row: {}", row.values());

        // Request the next row
        subscription.request(1);
    }

    @Override
    public synchronized void onError(Throwable t) {
        log.error("Received an error", t);
    }

    @Override
    public synchronized void onComplete() {
        log.info("Query has ended.");
        client.close();
    }
}