package partitioning.tool.kafka.admin;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;

import partitioning.tool.kafka.admin.describers.OffsetTopicsDescriber;
import partitioning.tool.kafka.admin.describers.PartitionAssignmentDescriber;
import partitioning.tool.kafka.common.PropertiesLoader;

public class ConsumerGroupStatus {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Required parameters: <config-file> <consumer-group>");
            return;
        }

        // load args
        final Properties properties = PropertiesLoader.load(args[0]);
        final String consumerGroup = args[1];

        AdminClient adminClient = AdminClient.create(properties);

        OffsetTopicsDescriber offsetTopicsDescriber = new OffsetTopicsDescriber(adminClient, consumerGroup);
        PartitionAssignmentDescriber partitionAssignmentDescriber = new PartitionAssignmentDescriber(adminClient, consumerGroup);

        while (true) {
            cleanOutput();

            offsetTopicsDescriber.refreshValues();
            partitionAssignmentDescriber.refreshValues();

            System.out.println("Date: " + LocalDateTime.now() + " - consumerGroup: " + consumerGroup + "\n");
            System.out.println("Topic\t\tPartition\tcurrentOffset\tendOffset\tclientId");
            for (var partition : offsetTopicsDescriber.getAllTopicsPartitions()) {

                final long currentOffset = offsetTopicsDescriber.getCurrentOffsetOrDefault(partition, -1L);
                final long endOffset = offsetTopicsDescriber.getEndOffsetOrDefault(partition, -1L);
                final String clientId = partitionAssignmentDescriber.getClientIdOrDefault(partition, "NOT_ASSIGNED");

                System.out.println(partition.topic()
                        + "\t\t" + partition.partition()
                        + "\t\t" + currentOffset
                        + "\t\t" + endOffset
                        + "\t\t" + clientId
                );
            }
            System.out.println(partitionAssignmentDescriber.printAssignment());

            waitHalfSecond();
        }
    }

    private static void cleanOutput() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }

    private static void waitHalfSecond() {
        try {
            Thread.sleep(500);
        } catch (final InterruptedException e) {
            System.err.println("Ops, sleep was interruped!" + e);
        }
    }
}
