package com.vmturbo.stitching.poststitching;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologyEntity;

public abstract class StorageProvisionedPostStitchingOperation extends OverprovisionCapacityPostStitchingOperation {

    public StorageProvisionedPostStitchingOperation() {
        super(EntitySettingSpecs.StorageOverprovisionedPercentage, CommodityType.STORAGE_AMOUNT,
            CommodityType.STORAGE_PROVISIONED);
    }

    @Override
    boolean shouldOverwriteCapacity() {
        return false;
    }

    /**
     * Post-stitching operation for the purpose of setting Storage Provisioned commodity capacities for
     * Storage entities.
     *
     * If the entity in question has a Storage Amount commodity, a Storage Provisioned commodity with
     * unset capacity, and a setting for storage overprovisioned percentage, then the Storage
     * Provisioned commodity's capacity is set to the Storage Amount commodity capacity multiplied by
     * the overprovisioned percentage.
     */
    public static class StorageEntityStorageProvisionedPostStitchingOperation extends
                                                    StorageProvisionedPostStitchingOperation {

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.STORAGE);
        }

    }

    /**
     * Post-stitching operation for the purpose of setting Storage Provisioned commodity capacities for
     * Logical Pool entities.
     *
     * If the entity in question has a Storage Amount commodity, a Storage Provisioned commodity with
     * unset capacity, and a setting for storage overprovisioned percentage, then the Storage
     * Provisioned commodity's capacity is set to the Storage Amount commodity capacity multiplied by
     * the overprovisioned percentage.
     */
    public static class LogicalPoolStorageProvisionedPostStitchingOperation extends
                                            StorageProvisionedPostStitchingOperation {

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.LOGICAL_POOL);
        }

    }

    /**
     * Post-stitching operation for the purpose of setting Storage Provisioned commodity capacities for
     * Disk Array entities.
     *
     * If the entity in question has a Storage Amount commodity, a Storage Provisioned commodity with
     * unset capacity, and a setting for storage overprovisioned percentage, then the Storage
     * Provisioned commodity's capacity is set to the Storage Amount commodity capacity multiplied by
     * the overprovisioned percentage.
     */
    public static class DiskArrayStorageProvisionedPostStitchingOperation extends
                                                    StorageProvisionedPostStitchingOperation {

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.DISK_ARRAY);
        }
    }
}
