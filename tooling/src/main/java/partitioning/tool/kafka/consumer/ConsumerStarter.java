package partitioning.tool.kafka.consumer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import partitioning.tool.kafka.common.PropertiesLoader;

public class ConsumerStarter {

    private static final List<String> NAME_PREFIX = List.of("APE", "BAT", "BEE", "CAT", "DOG", "GNU", "PIG", "RAT");

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerStarter.class);
    private static String TOPIC_PATTERN = "topic-.*";

    public static void main(final String[] args) throws IOException {
        if (args.length < 5) {
            System.err.println("Required parameters: <config-file> <group-id> <strategyClass> <static-assignment>");
            return;
        }

        // load configs
        final Properties properties = PropertiesLoader.load(args[0]);
        String groupId = args[1];
        final String partitionStrategy = args[2];
        final String instanceId = args[3];
        final boolean setStatic = Boolean.parseBoolean(args[4]);

        // extra configs
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionStrategy);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, instanceId);
        if (setStatic) {
            properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
        }

        final Consumer consumer = new Consumer(properties, TOPIC_PATTERN, 30);

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
        Random random = new Random();

        final String prefix = NAME_PREFIX.get(random.nextInt(NAME_PREFIX.size()));
        final char letter = (char) (leftLimit + random.nextInt(rightLimit + 1 - leftLimit));

        return prefix + "-" + letter + letter + letter + letter + letter;
    }
}
