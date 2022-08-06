package partitioning.tool.kafka.consumer;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import partitioning.tool.kafka.common.PropertiesLoader;

public class ConsumerStarter {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerStarter.class);

    public static void main(final String[] args) throws IOException {
        if (args.length != 5) {
            System.err.println("Required parameters: <config-file> <group-id> <topic-pattern> <delay-per-polling-in-ms> <strategyClass>");
            return;
        }

        // load configs
        final Properties properties = PropertiesLoader.load(args[0]);
        String groupId = args[1];
        final String topicPattern = args[2];
        final int delay = Integer.parseInt(args[3]);
        final String partitionStrategy = args[4];

        // extra configs
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionStrategy);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, generateRandomId());

        final Consumer consumer = new Consumer(properties, topicPattern, delay);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // Adding a shutdown hook to clean up when the application exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stopConsumerBeforeExit(consumer, mainThread)));

        consumer.run();
    }

    private static void stopConsumerBeforeExit(final Consumer consumer, final Thread mainThread) {
        LOG.info("Detected a shutdown, let's exit by calling closing the consumer...");
        consumer.wakeUp();

        // join the main thread to give time to consumer to close correctly
        try {
            mainThread.join();
        } catch (InterruptedException e) {
            LOG.error("Oh man...", e);
        }
    }

    public static String generateRandomId() {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 5;
        Random random = new Random();

        return random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }
}
