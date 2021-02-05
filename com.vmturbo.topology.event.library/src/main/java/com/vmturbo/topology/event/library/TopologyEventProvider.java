package com.vmturbo.topology.event.library;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * A provider of {@link TopologyEvents}.
 */
public interface TopologyEventProvider {

    /**
     * Retrieves the set of topology events within the {@code eventWindow}, filtered by the
     * {@code eventFilter}.
     * @param eventWindow The event window.
     * @param eventFilter The event filter.
     * @return The {@link TopologyEvents} matching the event window and filter.
     */
    @Nonnull
    TopologyEvents getTopologyEvents(@Nonnull TimeInterval eventWindow,
                                     @Nonnull TopologyEventFilter eventFilter);

    /**
     * Registers a listener for any updates to the topology event store. An update does not necessarily
     * mean there are new events to process.
     * @param listener The listener to register.
     */
    void registerUpdateListener(@Nonnull TopologyEventUpdateListener listener);

    /**
     * A listener for updates to topology events.
     */
    interface TopologyEventUpdateListener {

        /**
         * A callback method invoked when the topology event store is updated. Invocation of the callback
         * does not necessarily mean there are new events to process.
         */
        void onTopologyEventUpdate();
    }

    /**
     * A filter of topology events based on the scope of the associated entity. Topology events passes
     * the filter must pass the intersection of all filter criteria.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface TopologyEventFilter {

        /**
         * A pass-through {@link TopologyEventFilter}.
         */
        TopologyEventFilter ALL = TopologyEventFilter.builder().build();

        /**
         * The set of entity OIDs.
         * @return The set of entity OIDs.
         */
        @Nonnull
        Set<Long> entityOids();

        /**
         * The set of account OIDs.
         * @return The set of account OIDs.
         */
        @Nonnull
        Set<Long> accountOids();

        /**
         * The set of region OIDs.
         * @return The set of region OIDs.
         */
        @Nonnull
        Set<Long> regionOids();

        /**
         * The set of service provider OIDs.
         * @return The set of service provider OIDs.
         */
        @Nonnull
        Set<Long> serviceProviderOids();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed {@link Builder} instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link TopologyEventFilter} instances.
         */
        class Builder extends ImmutableTopologyEventFilter.Builder {}
    }
}
