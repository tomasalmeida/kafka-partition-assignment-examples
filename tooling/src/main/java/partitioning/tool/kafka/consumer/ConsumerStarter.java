package partitioning.tool.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import partitioning.tool.kafka.common.PropertiesLoader;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class ConsumerStarter {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerStarter.class);

    public static void main(final String[] args) throws IOException, InterruptedException {
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

        final Consumer myConsumer = new Consumer(properties, topicPattern, delay);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // Adding a shutdown hook to clean up when the application exits
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOG.info("Detected a shutdown, let's exit by calling closing the consumer...");
                myConsumer.close();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    LOG.error("Oh man...", e);
                }
            }
        });

        myConsumer.run();
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
