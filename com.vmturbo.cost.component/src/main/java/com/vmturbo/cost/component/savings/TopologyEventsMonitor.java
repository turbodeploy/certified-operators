package com.vmturbo.cost.component.savings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.savings.TopologyEvent.Builder;
import com.vmturbo.cost.component.savings.TopologyEvent.EventType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Generate topology events based on differences between a discovered topology and the state of
 * monitored entities.
 */
public class TopologyEventsMonitor {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    @Nonnull private Map<Integer, Config> configuration;

    /**
     * Constructor.
     *
     * @param configuration configuration specifying which entity types and which aspects to monitor
     */
    public TopologyEventsMonitor(@Nonnull Map<Integer, Config> configuration) {
        this.configuration = Objects.requireNonNull(configuration);
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
            @Nonnull CloudTopology cloudTopology, long topologyTimestamp) {
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

    /**
     * Reurn a map containing the capacities of the provided set of commodity types.
     *
     * @param entityDTO entity DTO to search
     * @param commodityTypes set of commodity types to find capacities for.
     * @return map from commodity type to capacity.  If the requested commodity is missing, it is
     *      omitted from the map.
     */
    @Nonnull
    private Map<Integer, Double> getCommodityCapacities(TopologyEntityDTO entityDTO,
            Set<Integer> commodityTypes) {
        Map<Integer, Double> values = new HashMap<>();
        for (CommoditySoldDTO commSold : entityDTO.getCommoditySoldListList()) {
            if (commodityTypes.contains(commSold.getCommodityType().getType())) {
                values.put(commSold.getCommodityType().getType(), commSold.getCapacity());
            }
        }
        return values;
    }

    private ChangeResult handleMissingEntity(MonitoredEntity monitoredEntity) {
        // We do not support entity deletion events yet.
        return ChangeResult.EMPTY;
    }

    private ChangeResult handleExistingEntity(CloudTopology cloudTopology, long topologyTimestamp,
            MonitoredEntity monitoredEntity, TopologyEntityDTO discoveredEntity) {
        int entityType = discoveredEntity.getEntityType();
        Config config = configuration.get(entityType);
        if (config == null) {
            // We don't track this entity type, so it shouldn't have been in the entity state table.
            logger.debug("TEM not tracking entity type {} name = {}", entityType,
                    discoveredEntity.getDisplayName());
            return ChangeResult.EMPTY;
        }

        Optional<Long> newProviderId = Optional.empty();
        Map<Integer, Double> newUsage = new HashMap<>();

        if (config.monitorProviderChange) {
            // Get the discovered entity's provider ID.  This is different based on the entity type.
            Optional<TopologyEntityDTO> provider = Optional.empty();
            if (entityType == EntityType.VIRTUAL_MACHINE_VALUE) {
                provider = cloudTopology.getComputeTier(monitoredEntity.getEntityId());
            } else if (EntityType.VIRTUAL_VOLUME_VALUE == entityType) {
                provider = cloudTopology.getStorageTier(monitoredEntity.getEntityId());
            }
            if (provider.isPresent()) {
                if (provider.get().getOid() != monitoredEntity.getProviderId()) {
                    // The provider ID changed
                    newProviderId = Optional.of(provider.get().getOid());
                    logger.debug("Provider changed for entity ID {} from {} to {}",
                            monitoredEntity.getEntityId(),
                            monitoredEntity.getProviderId(), newProviderId.get());
                }
            } else {
                // The discovered provider doesn't exist.  See if there is a provider ID on the
                // monitored entity.
                if (monitoredEntity.getProviderId() != null) {
                    // Provider change to none.
                    newProviderId = Optional.of(null);
                }
            }
        }

        // Check for changed commodity usage
        Map<Integer, Double> discoveredCapacities =
                getCommodityCapacities(discoveredEntity, config.monitoredCommodities);
        if (!discoveredCapacities.equals(monitoredEntity.getCommodityUsage())) {
            newUsage = discoveredCapacities;
            logger.debug("Monitored commodity usage changed for {} from {} to {}",
                    monitoredEntity.getEntityId(), monitoredEntity.getCommodityUsage().toString(),
                    newUsage.toString());
        }

        if (newProviderId.isPresent() || !newUsage.isEmpty()) {
            // Something changed, so generate an event.
            Builder topologyEvent = new Builder()
                    .eventType(EventType.PROVIDER_CHANGE.getValue())
                    .entityOid(monitoredEntity.getEntityId())
                    .entityType(entityType)
                    .timestamp(topologyTimestamp);
            newProviderId.ifPresent(oid -> {
                monitoredEntity.setProviderId(oid);
                topologyEvent.providerOid(oid);
            });
            if (!newUsage.isEmpty()) {
                monitoredEntity.setCommodityUsage(newUsage);
                topologyEvent.commodityUsage(newUsage);
            }
            // Wrap the topology event in a SavingsEvent
            SavingsEvent savingsEvent = new SavingsEvent.Builder()
                    .timestamp(topologyTimestamp)
                    .entityId(monitoredEntity.getEntityId())
                    .topologyEvent(topologyEvent.build())
                    .build();

            return new ChangeResult(true, ImmutableList.of(savingsEvent));
        }
        return ChangeResult.EMPTY;
    }

    /**
     * Helper class for multi-value return of generateEvents().
     */
    static class ChangeResult {
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
    }

    /**
     * Configuration for the TEM.  Specifies which entity types and which aspects to monitor.
     */
    static class Config {
        private final boolean monitorProviderChange;
        private final Set<Integer> monitoredCommodities;

        Config(boolean monitorProviderChange, @Nonnull Set<Integer> monitoredCommodities) {
            this.monitorProviderChange = monitorProviderChange;
            this.monitoredCommodities = Objects.requireNonNull(monitoredCommodities);
        }
    }
}
