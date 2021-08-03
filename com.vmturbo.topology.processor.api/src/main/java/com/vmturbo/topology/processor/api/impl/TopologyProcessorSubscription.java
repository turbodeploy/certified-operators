package com.vmturbo.topology.processor.api.impl;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;

/**
 * Represents a subscription to a topic that the topology processor sends notifications for.
 * <p>
 * Allows overriding certain Kafka-specific settings.
 */
public class TopologyProcessorSubscription {
    private final Topic topic;
    private final Optional<StartFrom> startFrom;

    /**
     * Use {@link TopologyProcessorSubscription#forTopic(Topic)} or
     * {@link TopologyProcessorSubscription#forTopicWithStartFrom(Topic, StartFrom)}.
     */
    private TopologyProcessorSubscription(@Nonnull final Topic topic,
                                          @Nonnull final Optional<StartFrom> startFrom) {
        this.topic = topic;
        this.startFrom = startFrom;
    }

    /**
     * Create a subscription for a particular topic, with default settings.
     *
     * @param topic The topic to create the subscrion for.
     * @return The {@link TopologyProcessorSubscription}, which can be passed to
     *         {@link TopologyProcessorClientConfig#topologyProcessor(TopologyProcessorSubscription...)}.
     */
    @Nonnull
    public static TopologyProcessorSubscription forTopic(@Nonnull final Topic topic) {
        return new TopologyProcessorSubscription(topic, Optional.empty());
    }

    /**
     * Create a subscription for a particular topic, overriding the {@link StartFrom} settings.
     *
     * @param topic The topic to create the subscrion for.
     * @param startFrom The {@link StartFrom} setting to use.
     * @return The {@link TopologyProcessorSubscription}, which can be passed to
     *         {@link TopologyProcessorClientConfig#topologyProcessor(TopologyProcessorSubscription...)}.
     */
    @Nonnull
    public static TopologyProcessorSubscription forTopicWithStartFrom(@Nonnull final Topic topic,
                                                                      @Nonnull final StartFrom startFrom) {
        return new TopologyProcessorSubscription(topic, Optional.of(startFrom));
    }

    /**
     * @return the topic the subscription for.
     */
    @Nonnull
    public Topic getTopic() {
        return topic;
    }

    /**
     * @return An {@link Optional} containing the override for the {@link StartFrom} setting, if any.
     */
    @Nonnull
    public Optional<StartFrom> getStartFrom() {
        return startFrom;
    }

    @Override
    public int hashCode() {
        return topic.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (other instanceof TopologyProcessorSubscription) {
            return ((TopologyProcessorSubscription)other).topic == topic;
        } else {
            return false;
        }
    }

    /**
     * Contains the list of Topics.
     */
    public enum Topic {
        /**
         * Notifications topic.
         */
        Notifications,

        /**
         * LiveTopologies topic.
         */
        LiveTopologies,

        /**
         * PlanTopologies topic.
         */
        PlanTopologies,

        /**
         * TopologySummaries topic.
         */
        TopologySummaries,

        /**
         * EntitiesWithNewState topic.
         */
        EntitiesWithNewState,
        /**
         * External action info reported.
         */
        ExternalActionApprovalResponse,
        /**
         * External action state change. Could occur when action approval probe will approve/
         * reject action.
         */
        ExternalActionStateChange
    }
}
