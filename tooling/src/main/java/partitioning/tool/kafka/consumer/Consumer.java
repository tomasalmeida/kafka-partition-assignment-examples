package partitioning.tool.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class Consumer{
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
    private final KafkaConsumer consumer;
    private final String topicPattern;
    private final int delay;

    public Consumer(final Properties properties, final String topicPattern, int delay) {
        this.consumer = new KafkaConsumer<>(properties);
        this.topicPattern = topicPattern;
        this.delay = delay;
    }

    public void run() {
        try {
            Pattern compile = Pattern.compile(topicPattern);
            consumer.subscribe(compile);
            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                for (final ConsumerRecord<String, String> record : records) {
                    LOG.debug("value = [{}], timestamp = [{}], partition = [{}]",
                            record.value(), record.timestamp(), record.partition());
                }
                Thread.sleep(delay);
            }
        } catch (InterruptedException e) {
            LOG.error("Woke up by", e);
        } finally {
            consumer.close();
            LOG.error("Closed echo consumer for topicPattern [{}]", topicPattern);
        }
    }

    public void close() {
        LOG.info("Waking up echo consumer for topicPattern [{}]", topicPattern);
        consumer.wakeup();
    }
}
