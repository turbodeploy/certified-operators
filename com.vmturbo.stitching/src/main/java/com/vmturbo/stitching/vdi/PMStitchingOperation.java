package com.vmturbo.stitching.vdi;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Stitching operation to stitch the proxy PM sent by the VDI probe with the
 * actual PM discovered by underlying VC.
 */
public class PMStitchingOperation extends VDIStitchingOperation {

    public PMStitchingOperation() {
        super(EntityType.PHYSICAL_MACHINE,
                ImmutableSet.of(CommodityType.CLUSTER));
    }

}
