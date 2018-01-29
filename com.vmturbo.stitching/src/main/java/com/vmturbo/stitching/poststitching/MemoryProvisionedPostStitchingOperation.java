package com.vmturbo.stitching.poststitching;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Post-stitching operation for the purpose of setting Memory Provisioned commodity capacities for
 * physical machines.
 *
 * If the PM in question has a Memory commodity, a Memory Provisioned commodity,
 * and a setting for memory overprovisioned percentage, then the Memory Provisioned commodity's capacity
 * is set to the Memory commodity capacity multiplied by the overprovisioned percentage.
 */
public class MemoryProvisionedPostStitchingOperation extends OverprovisionCapacityPostStitchingOperation {

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE);
    }

    public MemoryProvisionedPostStitchingOperation() {
        super(EntitySettingSpecs.MemoryOverprovisionedPercentage,
            CommodityType.MEM, CommodityType.MEM_PROVISIONED);
    }

    @Override
    boolean shouldOverwriteCapacity() {
        return true;
    }
}
