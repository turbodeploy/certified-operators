package com.vmturbo.market.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityCosts;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.components.api.server.IMessageSenderFactory;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.component.api.impl.MarketComponentNotificationReceiver;

/**
 * Utility class to create notification senders on top of Kafka brokers for Market component.
 */
public class MarketKafkaSender {

    private MarketKafkaSender() {}

    /**
     * Creates {@link MarketNotificationSender} on top of the specified kafka message producer.
     *
     * @param kafkaMessageProducer kafka message producer to send data through
     * @return market notification sender.
     */
    public static MarketNotificationSender createMarketSender(
            @Nonnull IMessageSenderFactory kafkaMessageProducer) {
        return new MarketNotificationSender(
                kafkaMessageProducer.messageSender(
                        MarketComponentNotificationReceiver.PROJECTED_TOPOLOGIES_TOPIC,
                        MarketKafkaSender::generateProjectedTopologyMessageKey),
                kafkaMessageProducer.messageSender(
                        MarketComponentNotificationReceiver.PROJECTED_ENTITY_COSTS_TOPIC,
                        MarketKafkaSender::generateCostMessageKey),
                kafkaMessageProducer.messageSender(
                        MarketComponentNotificationReceiver.PROJECTED_ENTITY_RI_COVERAGE_TOPIC,
                        MarketKafkaSender::generateRICoverageMessageKey),
                kafkaMessageProducer.messageSender(
                        MarketComponentNotificationReceiver.ACTION_PLANS_TOPIC),
                kafkaMessageProducer.messageSender(
                        MarketComponentNotificationReceiver.ANALYSIS_SUMMARY_TOPIC),
                kafkaMessageProducer.messageSender(
                        MarketComponentNotificationReceiver.ANALYSIS_STATUS_NOTIFICATION_TOPIC));
    }


    /**
     * Generate a message key that will be used for all messages that are part of the same sequence.
     *
     * @param message to generate a key for
     * @return the string key to use for the kafka message
     */
    public static String generateTopologyMessageKey(Topology message) {
        return Long.toString(message.getTopologyId());
    }

    public static String generateCostMessageKey(ProjectedEntityCosts message) {
        return Long.toString(message.getProjectedTopologyId());
    }

    public static String generateRICoverageMessageKey(ProjectedEntityReservedInstanceCoverage message) {
        return Long.toString(message.getProjectedTopologyId());
    }

    public static String generateProjectedTopologyMessageKey(ProjectedTopology message) {
        return Long.toString(message.getTopologyId());
    }

}
