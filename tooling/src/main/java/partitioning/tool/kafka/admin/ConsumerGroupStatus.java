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
            System.out.println("Topic     Partition   currentOffset   end Offset   Client Id      instanceId");
            for (var partition : offsetTopicsDescriber.getAllTopicsPartitions()) {

                final long currentOffset = offsetTopicsDescriber.getCurrentOffsetOrDefault(partition, -1L);
                final long endOffset = offsetTopicsDescriber.getEndOffsetOrDefault(partition, -1L);
                final String clientId = partitionAssignmentDescriber.getClientId(partition);
                final String instanceId = partitionAssignmentDescriber.getInstanceId(partition);

                final String message = String.format("%-9s %6s     %13s   %10s   %11s     %10s",
                        partition.topic(), partition.partition(), currentOffset, endOffset, clientId, instanceId);
                System.out.println(message);
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
