package com.vmturbo.stitching.poststitching;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;

/**
 * Set transaction capacity value from settings.
 * Note: Keep this class for backward compatibility.
 */
public class SetTransactionsCapacityPostStitchingOperation extends SetAutoSetCommodityCapacityPostStitchingOperation {
    private final String operationName;

    public SetTransactionsCapacityPostStitchingOperation(
            @Nonnull final EntityType entityType,
            @Nonnull final ProbeCategory probeCategory,
            @Nonnull final String capacitySettingName,
            @Nonnull final String autoSetSettingName) {
        super(entityType, probeCategory, CommodityType.TRANSACTION, capacitySettingName, autoSetSettingName);
        operationName = String.join("_", getClass().getSimpleName(),
                probeCategory.getCategory(), entityType.toString());
    }

    @Nonnull
    @Override
    public String getOperationName() {
        return operationName;
    }
}
