package com.vmturbo.stitching.poststitching;

import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Post-stitching operation for the purpose of setting CPU Provisioned commodity capacities for
 * physical machines if not already set (falling under the umbrella of provisioned commodity
 * post-stitching operations).
 *
 * If the entity in question has a CPU commodity, a CPU Provisioned commodity,
 * and a setting for CPU overprovisioned percentage, then the CPU Provisioned commodity's capacity
 * is set to the CPU commodity capacity multiplied by the overprovisioned percentage.
 */
public class CpuProvisionedPostStitchingOperation extends PmComputeCommodityCapacityPostStitchingOperation {

    public CpuProvisionedPostStitchingOperation() {
        super(EntitySettingSpecs.CpuOverprovisionedPercentage,
            CommodityType.CPU, CommodityType.CPU_PROVISIONED);
    }

    @Override
    boolean shouldOverwriteCapacity() {
        return true;
    }
}
