package com.vmturbo.common.protobuf.utils;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;

/**
 * The utils for HyperConverged instances.
 */
public interface HCIUtils {
    /**
     * True if it's a vSAN storage.
     * @param entityImpl - impl of the entity we are dealing with.
     * @return true if we're dealing with a vSAN storage.
     */
    static boolean isVSAN(@Nonnull TopologyEntityImpl entityImpl)  {
        return entityImpl.getEntityType() == EntityType.STORAGE_VALUE  &&
            entityImpl.getTypeSpecificInfo().hasStorage()  &&
            entityImpl.getTypeSpecificInfo().getStorage().getStorageType() ==
                StorageType.VSAN;
    }

    /**
     * Checks whether the commodity type is of a vSAN storage commodity.
     * @param commodityType integer value for the commodity type
     * @return true if the commodity type is related to vSAN storage
     */
    static boolean isVSANRelatedCommodity(int commodityType) {
        return commodityType == CommodityType.STORAGE_AMOUNT_VALUE
                        || commodityType == CommodityType.STORAGE_PROVISIONED_VALUE
                        || commodityType == CommodityType.STORAGE_ACCESS_VALUE
                        || commodityType == CommodityType.STORAGE_LATENCY_VALUE;
    }
}
