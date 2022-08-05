package partitioning.tool.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;

public class Producer {

    public static final int NEVER_STOP = -1;
    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);
    private final KafkaProducer<Integer, String> producer;
    private final Duration delay;
    private final String[] topics;
    private final int totalMessages;
    private final Random random;

    public Producer(final Properties config, final String[] topics, final int totalMessages, final Duration delay) {
        this.producer = new KafkaProducer<>(config);
        this.topics = topics;
        this.totalMessages = totalMessages;
        this.delay = delay;
        this.random = new Random();
    }

    public void run() {
        int sentMessages = 0;
        int topicId = 0;
        while (shouldSendMessages(sentMessages)) {
            final int key = random.nextInt();
            final String message = generateMessage();
            final ProducerRecord<Integer, String> record = new ProducerRecord<>(topics[topicId], key, message);
            LOG.info("Sending message [{}] - [{}] sent to topic [{}]",record.key(), record.value(), record.topic());
            producer.send(record, this::logMessageSent);
            waitUntilDurationExpires();
            if (totalMessages != NEVER_STOP) {
                sentMessages++;
            }
            topicId = (topicId + 1) % topics.length;
        }
    }

    public void close() {
        LOG.info("Closing echo producer for topic [{}]", topics);
        producer.close();
    }

    private void waitUntilDurationExpires() {
        try {
            Thread.sleep(delay.toMillis());
        } catch (final InterruptedException e) {
            LOG.error("Ops, sleep was interruped!", e);
        }
    }

    private void logMessageSent(final RecordMetadata metadata, final Exception exception) {
        if (exception != null) {
            LOG.error("Error sending message to topic [{}]", metadata.topic(), exception);
        } else {
            LOG.debug("Message acknowledged to topic [{}]", metadata.topic());
        }
    }

    private String generateMessage() {
        return "Generated at " + LocalDateTime.now();
    }

    private boolean shouldSendMessages(final int sentMessages) {
        return totalMessages == NEVER_STOP || sentMessages <= totalMessages;
    }
}
