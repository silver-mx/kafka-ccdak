package dns.demo.kafka.util;

import dns.demo.kafka.java.streams.util.StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Slf4j
public class MiscUtils {

    /**
     * Fragment taken from https://github.com/linuxacademy/content-ccdak-kafka-streams/blob/master/src/main/java/com/linuxacademy/ccdak/streams/StatelessTransformationsMain.java
     *
     * @param supplier
     */
    public static void runUntilCancelled(Supplier<StreamUtils.StreamPair> supplier) {

        StreamUtils.StreamPair streamPair = supplier.get();
        try (KafkaStreams streams = streamPair.kafkaStreams()) {
            CountDownLatch latch = new CountDownLatch(1);

            // Attach a shutdown handler to catch control-c and terminate the application gracefully.
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    latch.countDown();
                }
            });

            try {
                latch.await();
            } catch (final Throwable e) {
                System.out.println(e.getMessage());
                System.exit(1);
            }

            log.info("The run has been cancelled ....");
            System.exit(0);
        }
    }

}
