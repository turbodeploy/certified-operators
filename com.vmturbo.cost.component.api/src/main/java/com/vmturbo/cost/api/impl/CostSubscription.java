package com.vmturbo.cost.api.impl;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentSubscription;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;
import com.vmturbo.cost.api.impl.CostSubscription.Topic;

/**
 * Represents a subscription to a topic in the cost component
 */
public class CostSubscription extends ComponentSubscription<Topic> {

    private CostSubscription(@Nonnull final Topic topic,
                             @Nonnull final StartFrom startFromOverride) {
        super(topic, startFromOverride);
    }

    /**
     * Creates a subscription instance for the specified topic
     * @param topic The {@link Topic} of the subscription
     * @return A new subscription
     */

    @Nonnull
    public static CostSubscription forTopic(@Nonnull final Topic topic) {
        return new CostSubscription(topic, null);
    }

    /**
     * Creates a new subscription instance for a topic with an explicit request on the starting
     * point of the message queue
     * @param topic The {@link Topic} of the subscription
     * @param startFrom The requested starting point in the message queue
     * @return A new subscription
     */
    @Nonnull
    public static CostSubscription forTopicWithStartFrom(
            @Nonnull final Topic topic,
            @Nonnull final StartFrom startFrom) {
        return new CostSubscription(topic, startFrom);
    }

    /**
     * Represents the supported message topics broadcast from the cost component
     */
    public enum Topic {
        /**
         * Represents notification of status changes (e.g. RI coverage or cost is received & available
         * for the source or projected topology)
         */
        COST_STATUS_NOTIFICATION
    }
}
