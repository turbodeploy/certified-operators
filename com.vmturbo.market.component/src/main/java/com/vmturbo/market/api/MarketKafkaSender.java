package com.vmturbo.market.api;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.server.KafkaMessageProducer;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.component.api.impl.MarketComponentClient;
import com.vmturbo.priceindex.api.PriceIndexNotificationSender;
import com.vmturbo.priceindex.api.impl.PriceIndexNotificationReceiver;

/**
 * Utility class to create notification senders on top of Kafka brokers for Market component.
 */
public class MarketKafkaSender {

    /**
     * Creates {@link MarketNotificationSender} on top of the specified kafka message producer.
     *
     * @param threadPool thread pool to use
     * @param kafkaMessageProducer kafka message producer to send data through
     * @return market notification sender.
     */
    public static MarketNotificationSender createMarketSender(@Nonnull ExecutorService threadPool,
            @Nonnull KafkaMessageProducer kafkaMessageProducer) {
        return new MarketNotificationSender(threadPool, kafkaMessageProducer.messageSender(
                MarketComponentClient.PROJECTED_TOPOLOGIES_TOPIC),
                kafkaMessageProducer.messageSender(MarketComponentClient.ACTION_PLANS_TOPIC));
    }

    /**
     * Creates {@link PriceIndexNotificationSender} on top of the specified kafka message producer.
     *
     * @param kafkaMessageProducer kafka message producer to send data through
     * @return price index notification sender.
     */
    public static PriceIndexNotificationSender createPriceIndexSender(
            @Nonnull KafkaMessageProducer kafkaMessageProducer) {
        return new PriceIndexNotificationSender(kafkaMessageProducer.messageSender(
                PriceIndexNotificationReceiver.PRICE_INDICES_TOPIC));
    }
}
