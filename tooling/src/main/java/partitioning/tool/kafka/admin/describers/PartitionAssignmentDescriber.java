package partitioning.tool.kafka.admin.describers;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;

import partitioning.tool.kafka.admin.TopicPartitionComparator;

public class PartitionAssignmentDescriber {
    private final String consumerGroup;
    private final AdminClient adminClient;
    private Map<TopicPartition, String> topicAssignmentMap;
    private Map<String, String> clientPartitionAssignment;

    public PartitionAssignmentDescriber(final AdminClient adminClient, final String consumerGroup) {
        this.adminClient = adminClient;
        this.consumerGroup = consumerGroup;
    }

    public String getClientIdOrDefault(final TopicPartition topicPartition, final String defaultValue) {
        return topicAssignmentMap.getOrDefault(topicPartition, defaultValue);
    }

    public String printAssignment() {
        return "\nCLIENT\t\tPARTITIONS\n" +
                clientPartitionAssignment.entrySet()
                        .stream()
                        .map(entry -> entry.getKey() + "\t" + entry.getValue())
                        .collect(Collectors.joining("\n"));
    }

    public void refreshValues() throws ExecutionException, InterruptedException {
        Collection<MemberDescription> memberDescriptions = adminClient.describeConsumerGroups(List.of(consumerGroup))
                .describedGroups()
                .get(consumerGroup)
                .thenApply(ConsumerGroupDescription::members)
                .get();

        topicAssignmentMap = mapTopicsToMember(memberDescriptions);
        clientPartitionAssignment = mapClientsToPartitions(memberDescriptions);
    }

    private Map<String, String> mapClientsToPartitions(final Collection<MemberDescription> memberDescriptions) {
        Map<String, String> clientMappings = new HashMap<>();

        for (MemberDescription memberDescription : memberDescriptions) {
            final String clientId = memberDescription.clientId();
            final String assignment = getAssignment(memberDescription.assignment());
            clientMappings.put(clientId, assignment);
        }
        return clientMappings;
    }

    private String getAssignment(final MemberAssignment assignment) {
        final Map<String, String> partitionsPerTopic = assignment.topicPartitions()
                .stream()
                .sorted(new TopicPartitionComparator())
                .collect(Collectors.groupingBy(
                        TopicPartition::topic,
                        Collectors.mapping(topicPartition -> String.valueOf(topicPartition.partition()), Collectors.joining(","))));
        return partitionsPerTopic
                .entrySet()
                .stream()
                .map(entry -> entry.getKey() + "(" + entry.getValue() + ")")
                .collect(Collectors.joining(" "));
    }

    private Map<TopicPartition, String> mapTopicsToMember(final Collection<MemberDescription> memberDescriptions) {
        Map<TopicPartition, String> topicPartitionMap = new HashMap<>();
        for (MemberDescription memberDescription : memberDescriptions) {
            final String clientId = memberDescription.clientId();
            memberDescription
                    .assignment()
                    .topicPartitions()
                    .forEach(topicPartition -> topicPartitionMap.put(topicPartition, clientId));
        }
        return topicPartitionMap;
    }
}
