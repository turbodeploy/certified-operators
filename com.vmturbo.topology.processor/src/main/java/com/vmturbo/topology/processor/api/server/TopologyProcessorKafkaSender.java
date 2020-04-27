package com.vmturbo.topology.processor.api.server;

import java.time.Clock;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.components.api.server.IMessageSenderFactory;
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
     * @param clock System clock to use.
     * @return topology processor notification sender.
     */
    public static TopologyProcessorNotificationSender create(
            @Nonnull final ExecutorService threadPool,
            @Nonnull final IMessageSenderFactory kafkaMessageProducer,
            @Nonnull final Clock clock) {
        return new TopologyProcessorNotificationSender(threadPool, clock,
                kafkaMessageProducer.messageSender(
                        TopologyProcessorClient.TOPOLOGY_LIVE, TopologyProcessorKafkaSender::generateMessageKey),
                kafkaMessageProducer.messageSender(
                        TopologyProcessorClient.TOPOLOGY_USER_PLAN, TopologyProcessorKafkaSender::generateMessageKey),
                kafkaMessageProducer.messageSender(
                        TopologyProcessorClient.TOPOLOGY_SCHEDULED_PLAN, TopologyProcessorKafkaSender::generateMessageKey),
                kafkaMessageProducer.messageSender(TopologyProcessorClient.NOTIFICATIONS_TOPIC),
                kafkaMessageProducer.messageSender(TopologyProcessorClient.TOPOLOGY_SUMMARIES));
    }

    /**
     * Generate a message key that will be used for all messages that are part of the same sequence.
     *
     * @param message to generate a key for
     * @return the string key to use for the kafka message
     */
    public static String generateMessageKey(Topology message) {
        return Long.toString(message.getTopologyId());
    }
}
