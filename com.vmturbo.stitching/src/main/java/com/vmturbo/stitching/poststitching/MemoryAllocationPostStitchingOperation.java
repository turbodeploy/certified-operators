package com.vmturbo.stitching.poststitching;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Post-stitching operation for the purpose of setting Memory Allocation commodity capacities for
 * physical machines if not already set.
 *
 * If the entity in question has a Memory commodity, a Memory Allocation commodity with unset
 * capacity, and a setting for memory overprovisioned percentage, then the Memory Allocation
 * commodity's capacity is set to the Memory commodity capacity multiplied by the
 * overprovisioned percentage.
 */
public class MemoryAllocationPostStitchingOperation extends OverprovisionCapacityPostStitchingOperation {

    public MemoryAllocationPostStitchingOperation() {
        super(EntitySettingSpecs.MemoryOverprovisionedPercentage, CommodityType.MEM,
            CommodityType.MEM_ALLOCATION);
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE);
    }

    @Override
    boolean shouldOverwriteCapacity() {
        return false;
    }
}
