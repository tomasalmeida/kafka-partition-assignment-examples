package partitioning.tool.kafka.admin.describers;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
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
    private Map<TopicPartition, ClientDetails> clientDetailsPerPartitionMap;
    private Map<String, String> assignedPartitionsPerClient;

    public PartitionAssignmentDescriber(final AdminClient adminClient, final String consumerGroup) {
        this.adminClient = adminClient;
        this.consumerGroup = consumerGroup;
    }

    public String getClientId(final TopicPartition topicPartition) {
        return getProperty(topicPartition, ClientDetails::getClientId);
    }

    public String getInstanceId(final TopicPartition topicPartition) {
        return getProperty(topicPartition, ClientDetails::getInstanceId);
    }

    private String getProperty(final TopicPartition topicPartition, Function<ClientDetails, String> getter) {
        String id = null;
        if (clientDetailsPerPartitionMap.containsKey(topicPartition)) {
            final ClientDetails clientDetails = clientDetailsPerPartitionMap.get(topicPartition);
            id = getter.apply(clientDetails);
        }
        return id != null ? id : "NOT_ASSIGNED";
    }

    public String printAssignment() {
        return "\nCLIENT\t\tPARTITIONS\n" +
                assignedPartitionsPerClient.entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(entry -> entry.getKey() + "\t" + entry.getValue())
                        .collect(Collectors.joining("\n"));
    }

    public void refreshValues() throws ExecutionException, InterruptedException {
        Collection<MemberDescription> memberDescriptions = adminClient.describeConsumerGroups(List.of(consumerGroup))
                .describedGroups()
                .get(consumerGroup)
                .thenApply(ConsumerGroupDescription::members)
                .get();

        clientDetailsPerPartitionMap = mapPartitionToClientDetails(memberDescriptions);
        assignedPartitionsPerClient = getAllPartitionAssignedPerClientId(memberDescriptions);
    }

    private Map<String, String> getAllPartitionAssignedPerClientId(final Collection<MemberDescription> memberDescriptions) {
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

    private Map<TopicPartition, ClientDetails> mapPartitionToClientDetails(final Collection<MemberDescription> memberDescriptions) {
        Map<TopicPartition, ClientDetails> clientDataPerPartition = new HashMap<>();
        for (MemberDescription memberDescription : memberDescriptions) {
            final ClientDetails clientDetails = new ClientDetails();
            clientDetails.clientId = memberDescription.clientId();
            clientDetails.instanceId = memberDescription.groupInstanceId().orElse(null);
            memberDescription
                    .assignment()
                    .topicPartitions()
                    .forEach(topicPartition -> clientDataPerPartition.put(topicPartition, clientDetails));
        }
        return clientDataPerPartition;
    }

    private static class ClientDetails {
        String clientId;
        String instanceId;

        public String getClientId() {
            return clientId;
        }

        public String getInstanceId() {
            return instanceId;
        }
    }
}
