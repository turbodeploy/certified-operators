package com.vmturbo.stitching.poststitching;

import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Post-stitching operation for the purpose of setting Storage Latency commodity capacities.
 *
 * If the entity in question has a Storage Latency commodity with capacity unset and a setting for
 * Latency Capacity, then the commodity's capacity is set to the capacity specified by the setting.
 */
public abstract class StorageLatencyPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity>
    performOperation(@Nonnull final Stream<TopologyEntity> entities,
                     @Nonnull final EntitySettingsCollection settingsCollection,
                     @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {

        entities.filter(entity ->
                entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
                    .anyMatch(this::isLatencyCommodityWithEmptyCapacity))
            .forEach(eligible -> {
                final Optional<Setting> latencyCapacity =
                    settingsCollection.getEntitySetting(eligible, EntitySettingSpecs.LatencyCapacity);
                if (latencyCapacity.isPresent()) {
                    resultBuilder.queueUpdateEntityAlone(eligible, entity ->
                        applyOperationToEntity(latencyCapacity.get(), entity)
                    );
                } else {
                    // TODO switched from warn to debug to reduce logging load - does it need more visibility?
                    logger.debug("Could not set Storage Latency capacity for entity {} ; " +
                        "no setting was found", eligible.getOid());
                }
            });
        return resultBuilder.build();
    }

    /**
     * Update all eligible Storage Latency commodities for an entity
     * @param setting the Storage Latency setting to use
     * @param entity the entity whose commodities will be updated
     */
    private void applyOperationToEntity(final Setting setting, final TopologyEntity entity) {
        entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
            .filter(this::isLatencyCommodityWithEmptyCapacity)
            .forEach(builder -> {
                builder.setCapacity(setting.getNumericSettingValue().getValue());
                logger.debug("Latency capacity for entity {} set to {}", entity.getOid(),
                    builder.getCapacity());
            });
    }

    /**
     * Determine whether a CommoditySoldDTO.Builder is eligible to be updated by this
     * post-stitching operation, i.e. whether it is of the correct type (StorageLatency) and
     * whether it has capacity unset (which sometimes presents as capacity == 0).
     *
     * @param builder the CommoditySoldDTO.Builder to test
     * @return true if the builder should be updated, and false otherwise
     */
    private boolean isLatencyCommodityWithEmptyCapacity(final CommoditySoldDTO.Builder builder) {
        return (builder.getCommodityType().getType() == CommodityType.STORAGE_LATENCY_VALUE) &&
            (!builder.hasCapacity() || builder.getCapacity() <= 0);
    }

    /**
     * Post-stitching operation for the purpose of setting Storage Latency commodity capacities
     * on storage entities.
     *
     * If the entity in question has a Storage Latency commodity with capacity unset and a setting for
     * Latency Capacity, then the commodity's capacity is set to the capacity specified by the setting.
     */
    public static class StorageEntityLatencyPostStitchingOperation extends
            StorageLatencyPostStitchingOperation {
        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.STORAGE);
        }
    }

    /**
     * Post-stitching operation for the purpose of setting Storage Latency commodity capacities on
     * Storage Controller entities.
     *
     * If the entity in question has a Storage Latency commodity with capacity unset and a setting for
     * Latency Capacity, then the commodity's capacity is set to the capacity specified by the setting.
     */
    public static class StorageControllerLatencyPostStitchingOperation extends StorageLatencyPostStitchingOperation {

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.STORAGE_CONTROLLER);
        }
    }

    /**
     * Post-stitching operation for the purpose of setting Storage Latency commodity capacities for
     * Logical Pool entities.
     *
     * If the entity in question has a Storage Latency commodity with capacity unset and a setting for
     * Latency Capacity, then the commodity's capacity is set to the capacity specified by the setting.
     */
    public static class LogicalPoolLatencyPostStitchingOperation extends
            StorageLatencyPostStitchingOperation {
        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.LOGICAL_POOL);
        }
    }

    /**
     * Post-stitching operation for the purpose of setting Storage Latency commodity capacities for
     * Disk Array entities.
     *
     * If the entity in question has a Storage Latency commodity with capacity unset and a setting for
     * Latency Capacity, then the commodity's capacity is set to the capacity specified by the setting.
     */
    public static class DiskArrayLatencyPostStitchingOperation extends
            StorageLatencyPostStitchingOperation {
        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.DISK_ARRAY);
        }
    }
}
