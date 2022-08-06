package partitioning.tool.kafka;

import partitioning.tool.kafka.admin.ConsumerGroupDescriber;
import partitioning.tool.kafka.consumer.ConsumerStarter;
import partitioning.tool.kafka.producer.ProducerStarter;

public class FallbackApp {

    public static void main(String[] args) {
        System.err.println("Choose the right app:");
        System.err.println("java -cp <jar> " + ProducerStarter.class.getCanonicalName());
        System.err.println("java -cp <jar> " + ConsumerStarter.class.getCanonicalName());
        System.err.println("java -cp <jar> " + ConsumerGroupDescriber.class.getCanonicalName());
    }
}