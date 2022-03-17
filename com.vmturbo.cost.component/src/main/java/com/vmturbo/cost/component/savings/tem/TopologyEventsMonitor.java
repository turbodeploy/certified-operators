package com.vmturbo.cost.component.savings.tem;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.savings.bottomup.MonitoredEntity;
import com.vmturbo.cost.component.savings.bottomup.SavingsEvent;
import com.vmturbo.cost.component.savings.bottomup.TopologyEvent.Builder;
import com.vmturbo.cost.component.savings.bottomup.TopologyEvent.EventType;

/**
 * Generate topology events based on differences between a discovered topology and the state of
 * monitored entities.
 */
public class TopologyEventsMonitor {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor.
     */
    public TopologyEventsMonitor() {
    }

    /**
     * Generate savings events for the list of monitored entities based on the discovered topology.
     *
     * @param monitoredEntity monitored entity to generate events for
     * @param cloudTopology discovered topology to compare monitored entities against
     * @param topologyTimestamp timestamp of discovered topology, which serves as the timestamp for
     *      all generated events.
     * @return result returning list of generated savings events and a boolean to indicate whether the
     *          monitored entity state changed.
     */
    public ChangeResult generateEvents(@Nonnull MonitoredEntity monitoredEntity,
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology, long topologyTimestamp) {
        // Get the discovered data for this entity
        Optional<TopologyEntityDTO> discoveredEntity =
                cloudTopology.getEntity(monitoredEntity.getEntityId());
        if (!discoveredEntity.isPresent()) {
            // Entity is no longer in this discovery and has potentially been removed.
            return handleMissingEntity(monitoredEntity);
        } else {
            // It is still here. Check for changes.
            // Handle existing entity
            return handleExistingEntity(cloudTopology, topologyTimestamp, monitoredEntity,
                    discoveredEntity.get());
        }
    }

    private ChangeResult handleMissingEntity(MonitoredEntity monitoredEntity) {
        // We do not support entity deletion events yet.
        return ChangeResult.EMPTY;
    }

    private ChangeResult handleExistingEntity(CloudTopology<TopologyEntityDTO> cloudTopology,
            long topologyTimestamp, MonitoredEntity monitoredEntity, TopologyEntityDTO discoveredEntity) {
        ProviderInfo oldProviderInfo = monitoredEntity.getProviderInfo();
        ProviderInfo discoveredProviderInfo = ProviderInfoFactory.getDiscoveredProviderInfo(
                cloudTopology, discoveredEntity);
        if (!discoveredProviderInfo.equals(oldProviderInfo)) {
            Builder topologyEvent = new Builder()
                    .eventType(EventType.PROVIDER_CHANGE.getValue())
                    .entityType(discoveredProviderInfo.getEntityType())
                    .entityOid(monitoredEntity.getEntityId())
                    .timestamp(topologyTimestamp)
                    .providerInfo(discoveredProviderInfo);
            // Wrap the topology event in a SavingsEvent
            SavingsEvent savingsEvent = new SavingsEvent.Builder()
                    .timestamp(topologyTimestamp)
                    .entityId(monitoredEntity.getEntityId())
                    .topologyEvent(topologyEvent.build())
                    .build();

            // Set the updated ProviderInfo into the state.
            monitoredEntity.setProviderInfo(discoveredProviderInfo);

            return new ChangeResult(true, ImmutableList.of(savingsEvent));
        }
        return ChangeResult.EMPTY;
    }

    /**
     * Helper class for multi-value return of generateEvents().
     */
    public static class ChangeResult {
        /**
         * Empty change result.
         */
        public static final ChangeResult EMPTY = new ChangeResult(false, new ArrayList<>());

        /**
         * If present, the monitored entity was changed.
         */
        boolean stateUpdated;

        /**
         * List of savings events that were generated.
         */
        @Nonnull List<SavingsEvent> savingsEvents;

        ChangeResult(boolean stateUpdated, @Nonnull List<SavingsEvent> savingsEvents) {
            this.stateUpdated = stateUpdated;
            this.savingsEvents = Objects.requireNonNull(savingsEvents);
        }

        /**
         * Get a boolean that indicates whether the monitored entity states has changed.
         *
         * @return true if the monitored entity states has changed
         */
        public boolean isStateUpdated() {
            return stateUpdated;
        }

        /**
         * Get the list of savings events.
         *
         * @return a list of savings events
         */
        @Nonnull
        public List<SavingsEvent> getSavingsEvents() {
            return savingsEvents;
        }
    }
}
