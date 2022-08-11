package partitioning.tool.kafka.admin.describers;

import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import partitioning.tool.kafka.admin.TopicPartitionComparator;

public class OffsetTopicsDescriber {

    private static final Logger LOG = LoggerFactory.getLogger(OffsetTopicsDescriber.class);
    private final AdminClient adminClient;
    private final String consumerGroup;
    private final ListOffsetsOptions readOptions;
    private ListOffsetsResult futureEndOffsetPerPartition;
    private Map<TopicPartition, OffsetAndMetadata> metadataPerPartitionForConsumerGroup;

    /**
     * get offset info of topics from a given consumer group
     *
     * @param adminClient   admin client
     * @param consumerGroup some user group
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public OffsetTopicsDescriber(final AdminClient adminClient, final String consumerGroup) throws ExecutionException, InterruptedException {
        this.adminClient = adminClient;
        this.consumerGroup = consumerGroup;
        this.readOptions = new ListOffsetsOptions();
        refreshValues();
    }

    public void refreshValues() throws ExecutionException, InterruptedException {
        final KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> partitionMetadataForConsumerGroup = adminClient
                .listConsumerGroupOffsets(consumerGroup)
                .partitionsToOffsetAndMetadata();
        final Map<TopicPartition, OffsetSpec> topicsWithOptionLatest = createMapInputForTopicsCheck(partitionMetadataForConsumerGroup);
        futureEndOffsetPerPartition = adminClient.listOffsets(topicsWithOptionLatest, readOptions);
        metadataPerPartitionForConsumerGroup = partitionMetadataForConsumerGroup.get();
    }

    public long getEndOffsetOrDefault(TopicPartition topicPartition, final long defaultValue) {
        try {
            return futureEndOffsetPerPartition.partitionResult(topicPartition)
                    .thenApply(ListOffsetsResult.ListOffsetsResultInfo::offset)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Unable to get end offset", e);
        }
        return defaultValue;
    }

    public List<TopicPartition> getAllTopicsPartitions() {
        return metadataPerPartitionForConsumerGroup.keySet()
                .stream()
                .sorted(new TopicPartitionComparator())
                .collect(Collectors.toList());
    }

    public long getCurrentOffsetOrDefault(final TopicPartition topicPartition, final long defaultValue) {
        if (metadataPerPartitionForConsumerGroup.containsKey(topicPartition)) {
            return metadataPerPartitionForConsumerGroup.get(topicPartition).offset();
        }
        return defaultValue;
    }

    private Map<TopicPartition, OffsetSpec> createMapInputForTopicsCheck(final KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> partitionMetadata) throws ExecutionException, InterruptedException {
        return partitionMetadata
                .thenApply(this::createPartitionLatestOffsetMap)
                .get();
    }

    private Map<TopicPartition, OffsetSpec> createPartitionLatestOffsetMap(final Map<TopicPartition, OffsetAndMetadata> groupOffsets) {
        return groupOffsets.keySet()
                .stream()
                .collect(toMap(topicPartition -> topicPartition, topicPartition -> OffsetSpec.latest()));
    }
}

