package com.vmturbo.stitching.poststitching;

import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

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
 * Base class supporting post-stitching operation for the purpose of setting commodity
 * capacities for Storage entities.
 */
public class StorageEntityCapacityPostStitchingOperation
        extends BaseEntityCapacityPostStitchingOperation implements PostStitchingOperation {

    private final CommodityType commodityType;
    private final EntitySettingSpecs entitySettingSpecs;

    /**
     * Create a new StorageEntityCapacityPostStitchingOperation.
     *
     * @param commodityType The commodity type
     * @param entitySettingSpecs The entity setting specs
     */
    public StorageEntityCapacityPostStitchingOperation(final CommodityType commodityType,
                                                       final EntitySettingSpecs entitySettingSpecs) {
        this.commodityType = commodityType;
        this.entitySettingSpecs = entitySettingSpecs;
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.STORAGE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {

        entities.forEach(storage -> {
            // The provider capacity, if set, overrides the setting-derived capacity AND any
            // capacity already set in the entity.
            Optional<Double> providerCapacity = getProviderCapacity(storage.getProviders(),
                    commodityType.getNumber(),
                    ImmutableSet.of(EntityType.LOGICAL_POOL_VALUE,
                            EntityType.DISK_ARRAY_VALUE));
            // If there is no storage access provider (which happens with hypervisor probes),
            // and the probe did not set an explicit storage latency capacity, derive
            // the storage latency capacity from the latency capacity setting.
            double capacity = providerCapacity.orElseGet(() -> getNumericPolicyValue(storage,
                    settingsCollection, entitySettingSpecs));
            setCapacity(resultBuilder, storage, commodityType.getNumber(), providerCapacity.isPresent(),
                    capacity);
        });
        return resultBuilder.build();
    }
}