package com.vmturbo.stitching.poststitching;

import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Post-stitching operation for the purpose of setting CPU Allocation commodity capacities for
 * physical machines if not already set.
 *
 * If the entity in question has a CPU commodity, a CPU Allocation commodity with unset capacity,
 * and a setting for CPU overprovisioned percentage, then the CPU Allocation commodity's capacity
 * is set to the CPU commodity capacity multiplied by the overprovisioned percentage.
 */
public class CpuAllocationPostStitchingOperation extends PmComputeCommodityCapacityPostStitchingOperation {

    public CpuAllocationPostStitchingOperation() {
        super(EntitySettingSpecs.CpuOverprovisionedPercentage, CommodityType.CPU,
            CommodityType.CPU_ALLOCATION);
    }

    @Override
    boolean shouldOverwriteCapacity() {
        return false;
    }
}
