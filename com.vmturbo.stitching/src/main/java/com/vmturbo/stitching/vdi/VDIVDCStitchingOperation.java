package com.vmturbo.stitching.vdi;

import java.util.Collections;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Stitching operation to stitch the VDI proxy VDC with the underlying VC VDC.
 */
public class VDIVDCStitchingOperation extends VDIStitchingOperation {

    public VDIVDCStitchingOperation() {
        super(EntityType.VIRTUAL_DATACENTER, Collections.emptySet());
    }
}
