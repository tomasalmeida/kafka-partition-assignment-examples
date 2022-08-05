package partitioning.tool.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import partitioning.tool.kafka.common.PropertiesLoader;

import java.io.IOException;
import java.util.Properties;

public class ConsumerStarter {

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


        final Consumer myConsumer = new Consumer(properties, topicPattern, delay);

        // Adding a shutdown hook to clean up when the application exits
        Runtime.getRuntime().addShutdownHook(new Thread(myConsumer::close));

        myConsumer.run();
    }
}
