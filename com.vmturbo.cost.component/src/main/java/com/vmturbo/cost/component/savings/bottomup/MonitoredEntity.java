package com.vmturbo.cost.component.savings.bottomup;

import javax.annotation.Nonnull;

import com.vmturbo.cost.component.savings.temold.ProviderInfo;

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
     * Get ProviderInfo.
     *
     * @return ProviderInfo
     */
    @Nonnull
    ProviderInfo getProviderInfo();

    /**
     * Set ProviderInfo.
     *
     * @param providerInfo ProviderInfo
     */
    void setProviderInfo(ProviderInfo providerInfo);
}
