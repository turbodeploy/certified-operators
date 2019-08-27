package com.vmturbo.stitching.vdi;

import java.util.Collections;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Stitching operation to stitch the VDI proxy storage with
 * the underlying VC Storage.
 */
public class VDIStorageStitchingOperation extends VDIStitchingOperation {

    public VDIStorageStitchingOperation() {
        super(EntityType.STORAGE, Collections.singleton(CommodityType.STORAGE_CLUSTER));
    }
}
