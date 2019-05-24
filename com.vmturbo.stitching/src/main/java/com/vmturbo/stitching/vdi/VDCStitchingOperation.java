package com.vmturbo.stitching.vdi;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class VDCStitchingOperation extends VDIStitchingOperation {


    public VDCStitchingOperation() {
        super(EntityType.VIRTUAL_DATACENTER,
                ImmutableSet.of(CommodityType.CPU_ALLOCATION, CommodityType.MEM_ALLOCATION));
    }

}
