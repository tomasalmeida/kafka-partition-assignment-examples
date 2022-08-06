package partitioning.tool.kafka.consumer;

import static java.util.stream.Collectors.toMap;

import partitioning.tool.kafka.common.PropertiesLoader;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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

        final Map<TopicPartition, OffsetSpec> endOffsets = getEndOffsets(consumerGroup, adminClient)
                .get();
        
        final ListOffsetsOptions readOptions = getReadOptionWithTimeout();

        while (true) {
            System.out.print("\033[H\033[2J");
            System.out.flush();

            final Map<TopicPartition, OffsetAndMetadata> groupOffsets = adminClient.listConsumerGroupOffsets(consumerGroup)
                    .partitionsToOffsetAndMetadata()
                    .get();

            final Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsetsMap = adminClient.listOffsets(endOffsets, readOptions)
                    .all()
                    .get();

            System.out.println(LocalDateTime.now());
            for (var partition : groupOffsets.keySet()) {
                final long endOffset = endOffsetsMap.get(partition).offset();
                System.out.println("Topic " + partition.topic()
                        + "\t\tpartition = " + partition.partition()
                        + "\t\tcurrentOffset = " + groupOffsets.get(partition).offset()
                        + "\t\tendOffset = " + endOffset
                );
            }

            final Collection<MemberDescription> membersDescriptions = adminClient.describeConsumerGroups(List.of(consumerGroup))
                    .all()
                    .thenApply(consumerGroups -> consumerGroups.get(consumerGroup))
                    .thenApply(ConsumerGroupDescription::members)
                    .get();

            for(MemberDescription membersDescription : membersDescriptions) {
                System.out.println("clientId = " + membersDescription.clientId()
                        + "\t\tASSIGNMENT = " + getAllAssignments(membersDescription.assignment())
                );
            }

            waitHalfSecond();
        }
    }

    private static String getAllAssignments(final MemberAssignment assignment) {
        final Map<String, String> partitionsPerTopic = assignment.topicPartitions()
                .stream()
                .collect(Collectors.groupingBy(
                        TopicPartition::topic,
                        Collectors.mapping(topicPartition -> String.valueOf(topicPartition.partition()), Collectors.joining(","))));
        return partitionsPerTopic
                .entrySet()
                .stream()
                .map(entry -> entry.getKey() + "(" + entry.getValue() + ")")
                .collect(Collectors.joining(" "));
    }

    private static KafkaFuture<Map<TopicPartition, OffsetSpec>> getEndOffsets(final String consumerGroup, final AdminClient adminClient) throws InterruptedException, ExecutionException {
        return adminClient.listConsumerGroupOffsets(consumerGroup)
                .partitionsToOffsetAndMetadata()
                .thenApply(ConsumerGroupDescriber::createPartitionLatestOffsetMap);
    }

    private static Map<TopicPartition, OffsetSpec> createPartitionLatestOffsetMap(final Map<TopicPartition, OffsetAndMetadata> groupOffsets) {
        final Map<TopicPartition, OffsetSpec> endOffsets = groupOffsets.keySet()
                .stream()
                .collect(toMap(topicPartition -> topicPartition, topicPartition -> OffsetSpec.latest()));
        return endOffsets;
    }

    private static ListOffsetsOptions getReadOptionWithTimeout() {
        final ListOffsetsOptions readOptions = new ListOffsetsOptions();
        readOptions.timeoutMs(100);
        return readOptions;
    }

    private static void waitHalfSecond() {
        try {
            Thread.sleep(500);
        } catch (final InterruptedException e) {
            System.err.println("Ops, sleep was interruped!" + e);
        }
    }
}
