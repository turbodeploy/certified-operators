package com.vmturbo.market.api;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.server.KafkaMessageProducer;
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
            @Nonnull KafkaMessageProducer kafkaMessageProducer) {
        return new MarketNotificationSender(kafkaMessageProducer.messageSender(
                        MarketComponentNotificationReceiver.PROJECTED_TOPOLOGIES_TOPIC),
                kafkaMessageProducer.messageSender(
                        MarketComponentNotificationReceiver.PROJECTED_ENTITY_COSTS_TOPIC),
                kafkaMessageProducer.messageSender(
                        MarketComponentNotificationReceiver.PROJECTED_ENTITY_RI_COVERAGE_TOPIC),
                kafkaMessageProducer.messageSender(
                        MarketComponentNotificationReceiver.PLAN_ANALYSIS_TOPOLOGIES_TOPIC),
                kafkaMessageProducer.messageSender(
                        MarketComponentNotificationReceiver.ACTION_PLANS_TOPIC),
                kafkaMessageProducer.messageSender(
                    MarketComponentNotificationReceiver.ANALYSIS_RESULTS));
    }
}
