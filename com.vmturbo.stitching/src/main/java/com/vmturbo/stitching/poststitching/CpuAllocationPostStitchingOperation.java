package com.vmturbo.stitching.poststitching;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Post-stitching operation for the purpose of setting CPU Allocation commodity capacities for
 * physical machines if not already set.
 *
 * If the PM in question has a CPU commodity, a CPU Allocation commodity with unset capacity,
 * and a setting for CPU overprovisioned percentage, then the CPU Allocation commodity's capacity
 * is set to the CPU commodity capacity multiplied by the overprovisioned percentage.
 */
public class CpuAllocationPostStitchingOperation extends OverprovisionCapacityPostStitchingOperation {

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE);
    }

    public CpuAllocationPostStitchingOperation() {
        super(EntitySettingSpecs.CpuOverprovisionedPercentage, CommodityType.CPU,
            CommodityType.CPU_ALLOCATION);
    }

    @Override
    boolean shouldOverwriteCapacity() {
        return false;
    }
}
