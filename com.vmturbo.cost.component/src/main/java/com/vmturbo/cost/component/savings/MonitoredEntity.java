package com.vmturbo.cost.component.savings;

import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Represents an entity that is being monitored by the Topology Events Monitor (TEM).
 */
public interface MonitoredEntity {
    /**
     * Get the entity ID.
     *
     * @return the entity ID.
     */
    long getEntityId();

    /**
     * Get the timestamp when the entity was first detected missing.
     *
     * @return timestamp in milliseconds.
     */
    Long getLastPowerOffTransition();

    /**
     * Set the timestamp when the entity was first detected missing.
     *
     * @param timestamp timestamp in milliseconds.
     */
    void setLastPowerOffTransition(Long timestamp);

    /**
     * Get provider ID.
     *
     * @return provider ID.
     */
    Long getProviderId();

    /**
     * Set provider ID.
     *
     * @param providerId provider ID.
     */
    void setProviderId(Long providerId);

    /**
     * Get commodity usage for tracked commodity types.
     *
     * @return map from commodity type to usage.  If there are no tracked commodities, an
     *      empty map is returned.
     */
    @Nonnull
    Map<Integer, Double> getCommodityUsage();

    /**
     * Set commodity usage for this entity.
     *
     * @param commodityUsage map of commodity type to usage for that commodity.
     */
    void setCommodityUsage(@Nonnull Map<Integer, Double> commodityUsage);
}
