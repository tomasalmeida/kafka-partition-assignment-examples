package partitioning.tool.kafka.admin;

import static java.util.stream.Collectors.toMap;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetTopicsDescriber {

    private static final Logger LOG = LoggerFactory.getLogger(OffsetTopicsDescriber.class);
    private final AdminClient adminClient;
    private final String consumerGroup;
    private Map<TopicPartition, OffsetSpec> topicsWithOptionLatest;
    private final ListOffsetsOptions readOptions;
    private ListOffsetsResult futureOffsets;

    public OffsetTopicsDescriber(final AdminClient adminClient, final String consumerGroup) throws ExecutionException, InterruptedException {
        this.adminClient = adminClient;
        this.consumerGroup = consumerGroup;
        this.topicsWithOptionLatest = createMapInputForTopicsCheck();
        this.readOptions = new ListOffsetsOptions();
    }

    public void refreshValues() throws ExecutionException, InterruptedException {
        if (topicsWithOptionLatest.keySet().size() == 0) {
            topicsWithOptionLatest = createMapInputForTopicsCheck();
        }
        this.futureOffsets = adminClient.listOffsets(topicsWithOptionLatest, readOptions);
    }

    public long getOffsetOrDefault(TopicPartition topicPartition, final long defaultValue) {
        try {
            return futureOffsets.partitionResult(topicPartition)
                    .thenApply(ListOffsetsResult.ListOffsetsResultInfo::offset)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Unable to get end offset", e);
        }
        return defaultValue;
    }

    private Map<TopicPartition, OffsetSpec> createMapInputForTopicsCheck() throws ExecutionException, InterruptedException {
        return adminClient.listConsumerGroupOffsets(consumerGroup)
                .partitionsToOffsetAndMetadata()
                .thenApply(this::createPartitionLatestOffsetMap)
                .get();
    }

    private Map<TopicPartition, OffsetSpec> createPartitionLatestOffsetMap(final Map<TopicPartition, OffsetAndMetadata> groupOffsets) {
        return groupOffsets.keySet()
                .stream()
                .collect(toMap(topicPartition -> topicPartition, topicPartition -> OffsetSpec.latest()));
    }
}

