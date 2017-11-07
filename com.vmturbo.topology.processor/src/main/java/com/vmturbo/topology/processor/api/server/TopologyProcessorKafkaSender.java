package com.vmturbo.topology.processor.api.server;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.server.KafkaMessageProducer;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;

/**
 * Utility class to create topology notification sender for Kafka configuration.
 */
public class TopologyProcessorKafkaSender {

    /**
     * Creates {@link TopologyProcessorNotificationSender} on top of Kafka connection.
     *
     * @param threadPool thread pool
     * @param kafkaMessageProducer kafka producer to send through
     * @return topology processor notification sender.
     */
    public static TopologyProcessorNotificationSender create(@Nonnull ExecutorService threadPool,
            @Nonnull KafkaMessageProducer kafkaMessageProducer) {
        return new TopologyProcessorNotificationSender(threadPool,
                kafkaMessageProducer.messageSender(
                        TopologyProcessorClient.TOPOLOGY_LIVE),
                kafkaMessageProducer.messageSender(
                        TopologyProcessorClient.TOPOLOGY_USER_PLAN),
                kafkaMessageProducer.messageSender(
                        TopologyProcessorClient.TOPOLOGY_SCHEDULED_PLAN),
                kafkaMessageProducer.messageSender(TopologyProcessorClient.NOTIFICATIONS_TOPIC));
    }
}
