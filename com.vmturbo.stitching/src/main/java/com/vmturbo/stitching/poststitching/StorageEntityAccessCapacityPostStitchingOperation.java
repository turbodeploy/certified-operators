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
 * Post-stitching operation for the purpose of setting Storage Access commodity capacities for
 * Storage entities. The entity's Storage Access capacity is set to that of its Disk Array or
 * Logical Pool provider. If its Disk Array or Logical Pool provider does not sell Storage Access
 * capacity, the Storage Access capacity is set from the IOPS setting directly.
 *
 * This operation must occur after any StorageAccessCapacityPostStitchingOperations
 * so that all possible providers have their capacity set properly.
 * TODO: It is assumed that an entity has only one of these, but this may not be the case.
 */
public class StorageEntityAccessCapacityPostStitchingOperation
                extends BaseEntityCapacityPostStitchingOperation implements PostStitchingOperation {
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
                            CommodityType.STORAGE_ACCESS_VALUE,
                            ImmutableSet.of(EntityType.LOGICAL_POOL_VALUE,
                                            EntityType.DISK_ARRAY_VALUE));
            // If there is no storage access provider (which happens with hypervisor probes),
            // and the probe did not set an explicit storage access capacity, derive
            // the storage access capacity from the IOPS capacity setting.
            double capacity = providerCapacity.orElseGet(() -> getNumericPolicyValue(storage,
                            settingsCollection, EntitySettingSpecs.IOPSCapacity));
            setCapacity(resultBuilder, storage, CommodityType.STORAGE_ACCESS_VALUE,
                            providerCapacity.isPresent(), capacity);
        });
        return resultBuilder.build();
    }
}
