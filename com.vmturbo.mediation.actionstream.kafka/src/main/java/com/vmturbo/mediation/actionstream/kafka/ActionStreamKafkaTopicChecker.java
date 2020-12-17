package com.vmturbo.mediation.actionstream.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

/**
 * Class using for validating of kafka target (checks presence of configured topic on kafka broker).
 */
public class ActionStreamKafkaTopicChecker {

    /**
     * {@link KafkaConsumer} consumes records from a Kafka cluster.
     */
    private final KafkaConsumer<String, String> consumer;

    /**
     * Constructor of {@link ActionStreamKafkaTopicChecker}.
     *
     * @param properties configuration properties for {@link KafkaConsumer}
     */
    public ActionStreamKafkaTopicChecker(@Nonnull Properties properties) {
        consumer = new KafkaConsumer<>(properties);
    }

    /**
     * Define is topic exists on kafka broker.
     *
     * @param topicName the name of the topic
     * @return true if topic is present, otherwise false
     */
    public boolean isTopicAvailable(@Nonnull final String topicName) {
        final Map<String, List<PartitionInfo>> listTopics = consumer.listTopics();
        return listTopics.containsKey(topicName);
    }

    /**
     * Close {@link KafkaConsumer}.
     */
    public void close() {
        consumer.close();
    }
}
