package partitioning.tool.kafka.admin;

import java.util.Comparator;

import org.apache.kafka.common.TopicPartition;

public class TopicPartitionComparator implements Comparator<TopicPartition> {
    @Override
    public int compare(final TopicPartition topicPartition1, final TopicPartition topicPartition2) {
        final int comparationResult = topicPartition1.topic().compareTo(topicPartition2.topic());
        return comparationResult != 0 ? comparationResult : topicPartition1.partition() - topicPartition2.partition();
    }
}
