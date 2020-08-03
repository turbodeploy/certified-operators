package com.vmturbo.market.component.api.impl;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;

/**
 * Represents a subscription to a topic that the market sends notifications for.
 * <p>
 * Allows overriding certain Kafka-specific settings.
 */
public class MarketSubscription {

    private final Topic topic;

    private final Optional<StartFrom> startFromOverride;

    /**
     * Use {@link MarketSubscription#forTopic(Topic)} or
     * {@link MarketSubscription#forTopicWithStartFrom(Topic, StartFrom)}.
     */
    private MarketSubscription(@Nonnull final Topic topic,
                              @Nonnull final Optional<StartFrom> startFromOverride) {
        this.topic = topic;
        this.startFromOverride = startFromOverride;
    }

    /**
     * Create a subscription for a particular topic, with default settings.
     *
     * @param topic The topic to create the subscrion for.
     * @return The {@link MarketSubscription}, which can be passed to
     *         {@link MarketClientConfig#marketComponent(MarketSubscription...)}.
     */
    @Nonnull
    public static MarketSubscription forTopic(@Nonnull final Topic topic) {
        return new MarketSubscription(topic, Optional.empty());
    }

    /**
     * Create a subscription for a particular topic, overriding the {@link StartFrom} settings.
     *
     * @param topic The topic to create the subscrion for.
     * @param startFrom The {@link StartFrom} setting to use.
     * @return The {@link MarketSubscription}, which can be passed to
     *         {@link MarketClientConfig#marketComponent(MarketSubscription...)}.
     */
    @Nonnull
    public static MarketSubscription forTopicWithStartFrom(@Nonnull final Topic topic,
                                                           @Nonnull final StartFrom startFrom) {
        return new MarketSubscription(topic, Optional.of(startFrom));
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
    public Optional<StartFrom> getStartFrom() {
        return startFromOverride;
    }

    @Override
    public int hashCode() {
        return topic.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (other instanceof MarketSubscription) {
            return ((MarketSubscription)other).topic == topic;
        } else {
            return false;
        }
    }


    public enum Topic {
        /**
         * Analysis Summary topic.
         */
        AnalysisSummary,
        /**
         * Action Plans topic.
         */
        ActionPlans,
        /**
         * Projected Topology topic.
         */
        ProjectedTopologies,
        /**
         * Projected Entity Costs topic.
         */
        ProjectedEntityCosts,
        /**
         * Projected Entity Ri Coverage topic.
         */
        ProjectedEntityRiCoverage,
        /**
         * Projected Plan Analysis topic.
         */
        PlanAnalysisTopologies,
        /**
         * Analysis Status topic.
         *
         * <p>Notifies Status of a market analysis run.
         */
        AnalysisStatusNotification;
    }
}
