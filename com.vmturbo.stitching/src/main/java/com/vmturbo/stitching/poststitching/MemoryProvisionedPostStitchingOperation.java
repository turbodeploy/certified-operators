package com.vmturbo.stitching.poststitching;

import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Post-stitching operation for the purpose of setting Memory Provisioned commodity capacities for
 * physical machines if not already set (falling under the umbrella of provisioned commodity
 * post-stitching operations).
 *
 * If the entity in question has a Memory commodity, a Memory Provisioned commodity,
 * and a setting for memory overprovisioned percentage, then the Memory Provisioned commodity's capacity
 * is set to the Memory commodity capacity multiplied by the overprovisioned percentage.
 */
public class MemoryProvisionedPostStitchingOperation extends PmComputeCommodityCapacityPostStitchingOperation {

    public MemoryProvisionedPostStitchingOperation() {
        super(EntitySettingSpecs.MemoryOverprovisionedPercentage,
            CommodityType.MEM, CommodityType.MEM_PROVISIONED);
    }

    @Override
    boolean shouldOverwriteCapacity() {
        return true;
    }
}
