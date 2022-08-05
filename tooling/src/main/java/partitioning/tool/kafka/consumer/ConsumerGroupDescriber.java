package partitioning.tool.kafka.consumer;

import partitioning.tool.kafka.common.PropertiesLoader;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConsumerGroupDescriber {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Required parameters: <config-file> <consumer-group>");
            return;
        }

        // load configs
        final Properties properties = PropertiesLoader.load(args[0]);
        final String consumerGroup = args[1];

        AdminClient adminClient = AdminClient.create(properties);

        while(true) {
            System.out.print("\033[H\033[2J");
            System.out.flush();

            Map<TopicPartition, OffsetAndMetadata> offsets = adminClient.listConsumerGroupOffsets(consumerGroup)
                    .partitionsToOffsetAndMetadata()
                    .get();

            System.out.println(LocalDateTime.now());
            for (var partition : offsets.keySet()) {
                System.out.println("Topic " + partition.topic() + ", partition = " + partition.partition() + ", offset = " + offsets.get(partition).offset());
            }
            waitOneSecond();
        }
    }

    private static void waitOneSecond() {
        try {
            Thread.sleep(1000);
        } catch (final InterruptedException e) {
            System.err.println("Ops, sleep was interruped!" + e);
        }
    }
}
