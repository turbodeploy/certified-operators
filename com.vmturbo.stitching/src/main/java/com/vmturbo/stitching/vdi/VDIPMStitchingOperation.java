package com.vmturbo.stitching.vdi;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Stitching operation to stitch the proxy PM sent by the VDI probe with the
 * actual PM discovered by underlying VC.
 */
public class VDIPMStitchingOperation extends VDIStitchingOperation {

    public VDIPMStitchingOperation() {
        super(EntityType.PHYSICAL_MACHINE,
                ImmutableSet.of(CommodityType.CLUSTER, CommodityType.VMPM_ACCESS));
    }
}
