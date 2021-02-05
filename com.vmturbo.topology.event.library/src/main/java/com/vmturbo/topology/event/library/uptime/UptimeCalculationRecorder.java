package com.vmturbo.topology.event.library.uptime;

import java.time.Duration;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.topology.event.library.TopologyEvents.TopologyEventLedger;

/**
 * A recorder of discrete steps in calculating entity uptime through {@link EntityUptimeCalculator}.
 */
public class UptimeCalculationRecorder {



    private EntitySelector entitySelector = EntitySelector.DEFAULT_SELECTOR;

    /**
     * Sets the entity selection.
     * @param entitySelector The new {@link EntitySelector}.
     */
    public void setEntitySelector(@Nullable EntitySelector entitySelector) {
        this.entitySelector = (entitySelector == null)
                ? EntitySelector.DEFAULT_SELECTOR
                : entitySelector;
    }

    /**
     * Clears the current entity selector.
     */
    public void clearEntitySelection() {
        this.entitySelector = EntitySelector.DEFAULT_SELECTOR;
    }

    /**
     * Records the start of a full uptime calculation.
     * @param eventLedger The event ledger.
     * @param uptimeWindow The uptime window.
     */
    public void recordCalculationStart(@Nonnull TopologyEventLedger eventLedger,
                                       @Nonnull TimeInterval uptimeWindow) {

        if (entitySelector.isSelected(eventLedger.entityOid())) {

        }
    }

    /**
     * Records the start of a cached uptime calculation.
     * @param baseUptime The base (precomputed) entity uptime info.
     * @param eventLedger The event ledger to be appended to {@code baseUptime}.
     * @param uptimeWindow The uptime window.
     */
    public void recordCalculationStart(@Nonnull CachedEntityUptime baseUptime,
                                       @Nonnull TopologyEventLedger eventLedger,
                                       @Nonnull TimeInterval uptimeWindow) {

        if (entitySelector.isSelected(eventLedger.entityOid())) {

        }
    }

    /**
     * Records an event prior to the uptime window.
     * @param entityOid The entity OID.
     * @param topologyEvent The topology event.
     * @param startState The current start state of the entity going into the uptime window.
     */
    public void recordPriorEvent(long entityOid,
                                 @Nonnull TopologyEvent topologyEvent,
                                 @Nonnull EntityState startState) {

    }

    /**
     * Records a skipped event. An event will be skipped if it occurs prior to a base precomputed/cached
     * entity uptime calculation.
     * @param entityOid The entity OID.
     * @param topologyEvent The skipped topology event.
     */
    public void recordSkippedEvent(long entityOid,
                                   @Nonnull TopologyEvent topologyEvent) {

    }

    /**
     * Records the finalized uptime calculation.
     * @param cachedUptime The finalized precomputed/cached uptime info.
     * @param uptime The uptime duration.
     */
    public void recordUptime(@Nonnull CachedEntityUptime cachedUptime,
                             @Nonnull Duration uptime) {

    }

    /**
     * An entity selector for recording uptime calculation steps.
     */
    @FunctionalInterface
    public interface EntitySelector {

        /**
         * The default selector, which is to skip all entities.
         */
        EntitySelector DEFAULT_SELECTOR = (entityOid) -> false;

        /**
         * Whether the entity is selected.
         * @param entityOid The entity OID.
         * @return Whether the entity is selected.
         */
        boolean isSelected(long entityOid);
    }

    private String convertLedgerToString(@Nonnull TopologyEventLedger eventLedger) {

        return null;
    }
}
