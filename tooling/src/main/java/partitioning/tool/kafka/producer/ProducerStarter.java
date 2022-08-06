package partitioning.tool.kafka.producer;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import partitioning.tool.kafka.common.PropertiesLoader;

public class ProducerStarter {

    public static void main(final String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Required parameters: <config-file> <delay-per-message-in-ms> <topics-comma-separated>");
            return;
        }

        // load configs
        final Properties properties = PropertiesLoader.load(args[0]);
        final Duration delay = Duration.ofMillis(Integer.parseInt(args[1]));
        String[] topics = args[2].split(",");

        final Producer myProducer = new Producer(properties, topics, Producer.NEVER_STOP, delay);

        // Adding a shutdown hook to clean up when the application exits
        Runtime.getRuntime().addShutdownHook(new Thread(myProducer::close));

        myProducer.run();
    }
}
