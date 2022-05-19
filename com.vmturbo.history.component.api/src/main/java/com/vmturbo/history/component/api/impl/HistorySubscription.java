package com.vmturbo.history.component.api.impl;

import javax.annotation.Nonnull;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.vmturbo.components.api.client.ComponentSubscription;
import com.vmturbo.components.api.client.KafkaMessageConsumer;

/**
 * Represents a subscription to a topic in the history component.
 */
public class HistorySubscription extends ComponentSubscription<HistorySubscription.Topic> {

    protected HistorySubscription(@NotNull Topic topic, @Nullable KafkaMessageConsumer.TopicSettings.StartFrom startFromOverride) {
        super(topic, startFromOverride);
    }

    /**
     * Creates a subscription instance for the specified topic.
     * @param topic The {@link Topic} of the subscription
     * @return A new subscription
     */
    @Nonnull
    public static HistorySubscription forTopic(@Nonnull final Topic topic) {
        return new HistorySubscription(topic, null);
    }

    /**
     * Represents the supported message topics broadcast from the history component.
     */
    public enum Topic {
        /**
         * Represents notification of status changes (e.g. RI coverage or cost is received & available
         * for the source or projected topology)
         */
        HISTORY_VOL_NOTIFICATION
    }
}
