package com.vmturbo.stitching.poststitching;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

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
public class StorageLatencyPostStitchingOperation extends BaseEntityCapacityPostStitchingOperation
                implements PostStitchingOperation {
    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
                    @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.multiEntityTypesScope(ImmutableList.of(EntityType.STORAGE,
                        EntityType.DISK_ARRAY, EntityType.STORAGE_CONTROLLER,
                        EntityType.LOGICAL_POOL, EntityType.VIRTUAL_VOLUME));
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
                    @Nonnull final Stream<TopologyEntity> entities,
                    @Nonnull final EntitySettingsCollection settingsCollection,
                    @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {

        entities.forEach(entity -> {
            final double policyCapacity = getNumericPolicyValue(entity, settingsCollection,
                            EntitySettingSpecs.LatencyCapacity);
            setCapacity(resultBuilder, entity, CommodityType.STORAGE_LATENCY_VALUE, false,
                            policyCapacity);
        });
        return resultBuilder.build();
    }
}
