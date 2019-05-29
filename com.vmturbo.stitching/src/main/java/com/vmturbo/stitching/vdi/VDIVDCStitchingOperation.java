package com.vmturbo.stitching.vdi;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Stitching operation to stitch the VDI proxy VDC with the underlying VC VDC.
 */
public class VDIVDCStitchingOperation extends VDIStitchingOperation {


    public VDIVDCStitchingOperation() {
        super(EntityType.VIRTUAL_DATACENTER,
                ImmutableSet.of(CommodityType.CPU_ALLOCATION, CommodityType.MEM_ALLOCATION));
    }

}
