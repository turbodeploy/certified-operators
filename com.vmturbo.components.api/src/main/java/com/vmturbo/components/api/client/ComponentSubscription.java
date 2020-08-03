package com.vmturbo.components.api.client;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;

/**
 * A base class for component-specific subscriptions.
 * @param <TOPIC> The component-specific TOPIC enum
 */
public abstract class ComponentSubscription<TOPIC extends Enum> {

    private final TOPIC topic;

    private final StartFrom startFromOverride;

    protected ComponentSubscription(@Nonnull final TOPIC topic,
                                    @Nullable final StartFrom startFromOverride) {
        this.topic = topic;
        this.startFromOverride = startFromOverride;
    }

    /**
     * Get the topic of this subscription. The topic is expected to be a component-specific enum
     * @return A component-specific enum identifying the topic
     */
    @Nonnull
    public TOPIC getTopic() {
        return topic;
    }

    /**
     * @return An {@link Optional} containing the override for the {@link StartFrom} setting, if any.
     */
    public Optional<StartFrom> getStartFrom() {
        return Optional.ofNullable(startFromOverride);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return topic.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        } else if (other instanceof ComponentSubscription) {
            return ((ComponentSubscription)other).topic == topic;
        } else {
            return false;
        }
    }
}
